pub mod rdma_target {
    use std::ffi::CStr;
use crate::memory::DmaSlice;
    use crate::rdma::buffer_manager::{BufferManager, ThreadSafeDmaHandle};
    use crate::rdma::capsule::capsule::CapsuleContext;
    use crate::rdma::rdma_common::rdma_binding;
    use crate::rdma::rdma_common::rdma_common::{get_rdma_event_type_string, process_cm_event, ClientRdmaContext, RdmaTransportError, Sendable, MAX_WR};
    use crate::rdma::rdma_common::*;
    use crate::rdma::rdma_work_manager::RdmaWorkManager;
    use crate::{NvmeDevice, NvmeQueuePair, QUEUE_LENGTH};
    use bounded_spsc_queue::{make, Consumer, Producer};
    use std::collections::HashMap;
    use std::hint::spin_loop;
    use std::net::Ipv4Addr;
    use std::ops::Add;
    use std::ptr::null_mut;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::{io, mem, ptr, thread};
    use std::fs::File;
    use tracing::{span, Dispatch, Level};
    use tracing_perfetto::PerfettoLayer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    #[repr(transparent)]
    pub struct SendableCmEvent(pub *mut rdma_binding::rdma_cm_event);

    // SAFETY: we guarantee only one thread has access to this pointer.
    unsafe impl Send for SendableCmEvent {}
    unsafe impl Sync for SendableCmEvent {} // only if you're putting it in Arc/Mutex

    type InternalNVMeCommandType = (
        u16, // cid
        usize, // start offset
        u64, // nvme LBA
        bool, // is write
        usize // data len
    );

    type WrIdType = u16;

    struct RDMAWorkRequest {
        wr_id: u16,
        sge: rdma_binding::ibv_sge,
        mode: Option<rdma_binding::ibv_wr_opcode>,
        remote_info: Option<(u64, u32)>, // addr - rkey
    }

    #[repr(transparent)]
    pub struct CmEventPtr(pub *mut rdma_binding::rdma_cm_event);

    unsafe impl Send for CmEventPtr {}
    unsafe impl Sync for CmEventPtr {}

    #[repr(transparent)]
    pub struct RdmaEventChannelPtr(pub *mut rdma_binding::rdma_event_channel);

    unsafe impl Send for RdmaEventChannelPtr {}
    unsafe impl Sync for RdmaEventChannelPtr {}

    struct TargetRdmaContext {
        name: String,
        cm_event_channel: Arc<RdmaEventChannelPtr>,
        cm_id: *mut rdma_binding::rdma_cm_id,
    }
    impl TargetRdmaContext {
        pub fn new(
            name: String,
            cm_event_channel: *mut rdma_binding::rdma_event_channel,
            cm_id: *mut rdma_binding::rdma_cm_id,
        ) -> Self {
            TargetRdmaContext {
                name,
                cm_event_channel: Arc::from(RdmaEventChannelPtr(cm_event_channel)),
                cm_id,
            }
        }
    }
    impl Drop for TargetRdmaContext {
        fn drop(&mut self) {
            // unsafe {
            //     let rc = rdma_binding::ibv_destroy_qp(self.cm_id.qp);
            //     if rc != 0 {
            //         eprintln!("{}: ibv_destroy_qp() failed.", self.name)
            //     }
            // }

            unsafe {
                let rc = rdma_binding::rdma_destroy_id(self.cm_id);
                if rc != 0 {
                    eprintln!("{}: rdma_destroy_id() failed.", self.name);
                }
            }

            unsafe {
                rdma_binding::rdma_destroy_event_channel(self.cm_event_channel.0);
            }
        }
    }

    pub struct RdmaTarget {
        server_sockaddr: rdma_binding::sockaddr_in,
        ctx: TargetRdmaContext,
        client_handlers: Vec<(JoinHandle<()>, JoinHandle<()>)>,
        client_thread_signal: HashMap<String, Arc<AtomicBool>>,
        buffer_manager: Arc<BufferManager>,
        nvme_device_arc: Arc<Mutex<NvmeDevice>>,
        block_size: usize
    }

    fn alloc_pd() -> Result<Box<rdma_binding::ibv_pd>, RdmaTransportError> {
        let mut num_devices: i32 = 0;
        let device_lists = unsafe { rdma_binding::ibv_get_device_list(&mut num_devices) };
        if device_lists.is_null() || num_devices <= 0 {
            return Err(RdmaTransportError::OpFailed(
                "Failed to get RDMA device list".into(),
            ));
        }
        let device = unsafe { *device_lists };
        if device.is_null() {
            return Err(RdmaTransportError::OpFailed(
                "Failed to get RDMA device".into(),
            ));
        }
        let context = unsafe { rdma_binding::ibv_open_device(device) };
        if context.is_null() {
            return Err(RdmaTransportError::OpFailed(
                "Failed to open RDMA device".into(),
            ));
        }
        let pd = unsafe { rdma_binding::ibv_alloc_pd(context) };
        if pd.is_null() {
            return Err(RdmaTransportError::OpFailed(
                "Failed to allocate device PD".into(),
            ));
        }

        unsafe { rdma_binding::ibv_free_device_list(device_lists) };

        Ok(unsafe { Box::from_raw(pd) })
    }

    impl RdmaTarget {
        pub fn new(
            ipv4addr: Ipv4Addr,
            reserved_memory: usize,
            block_size: usize,
            device_pci_addr: &String
        ) -> Result<Self, RdmaTransportError> {
            let mut sockaddr = rdma_binding::sockaddr_in {
                sin_family: libc::AF_INET as u16,
                sin_port: 4421u16.to_be(),
                sin_addr: rdma_binding::in_addr {
                    s_addr: u32::from(ipv4addr).to_be(), // Bind to all interfaces
                },
                sin_zero: [0; 8],
            };
            let socket_addr =
                &mut sockaddr as *mut rdma_binding::sockaddr_in as *mut rdma_binding::sockaddr;

            let err_msg: String;
            let server_name = "Server Context";
            let cm_event_channel;
            let mut cm_id = null_mut();

            /*  Open a channel used to report asynchronous communication event */
            unsafe {
                debug_println_verbose!("server setup: creating event channel...");
                cm_event_channel = rdma_binding::rdma_create_event_channel();

                if cm_event_channel.is_null() {
                    err_msg = format!("{}: Failed to create event channel", server_name);
                    return Err(RdmaTransportError::OpFailed(err_msg));
                }

                debug_println_verbose!("server setup: event channel is created.");
            }

            // rdma_cm_id is the connection identifier (like socket) which is used
            // to define an RDMA connection.
            unsafe {
                debug_println_verbose!("server setup: creating CM ID...");
                let rc = rdma_binding::rdma_create_id(
                    cm_event_channel,
                    &mut cm_id,
                    ptr::null_mut(),
                    rdma_binding::rdma_port_space_RDMA_PS_TCP,
                );
                if rc != 0 {
                    err_msg = format!("{}: Failed to CM ID. code: {}", server_name, rc);
                    return Err(RdmaTransportError::OpFailed(err_msg));
                }
                debug_println_verbose!("server setup: CM ID is created.");
            }

            unsafe {
                debug_println_verbose!(
                    "server setup: binding rdma cm id to the socket credentials..."
                );
                let rc = rdma_binding::rdma_bind_addr(cm_id, socket_addr);
                if rc != 0 {
                    err_msg = format!(
                        "{}: Failed to bind CM ID to the socket credential",
                        server_name
                    );
                    return Err(RdmaTransportError::OpFailed(err_msg));
                }
                debug_println_verbose!("server setup: CM ID is bind.");
            }

            unsafe {
                let rc = rdma_binding::rdma_listen(cm_id, 1);
                if rc != 0 {
                    err_msg = format!(
                        "{}: rdma_listen failed to listen on server address. code: {}",
                        server_name, rc
                    );
                    return Err(RdmaTransportError::OpFailed(err_msg));
                }
            }

            debug_println_verbose!(
                "Server is listening at {}:{}",
                Ipv4Addr::from(u32::from_be(sockaddr.sin_addr.s_addr)),
                sockaddr.sin_port.to_be()
            );

            Ok(RdmaTarget {
                server_sockaddr: sockaddr,
                client_handlers: vec![],
                client_thread_signal: Default::default(),
                buffer_manager: Arc::from(BufferManager::new(reserved_memory, block_size).unwrap()),
                ctx: TargetRdmaContext::new(server_name.to_string(), cm_event_channel, cm_id),
                nvme_device_arc: Arc::from(Mutex::from(crate::init(&device_pci_addr).map_err(|_| {
                    RdmaTransportError::OpFailed("Failed to initiate NVMe Device".into())
                })?)),
                block_size
            })
        }

        pub fn run(&mut self) -> Result<i32, RdmaTransportError> {
            let err_msg: String;
            let mut client_number = 0;
            let filename = "target-trace.perfetto".to_string();
            let file = File::create(&filename).expect("failed to create trace file");
            let layer = PerfettoLayer::new(Mutex::new(file));
            let subscriber = Registry::default().with(layer);
            tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

            // handle
            loop {
                let cm_event_ptr = {
                    let mut cm_event = ptr::null_mut();
                    let rc = process_cm_event(self.ctx.cm_event_channel.0, &mut cm_event)?;
                    if rc != 0 {
                        err_msg = format!("{}: Failed to get cm event: {}", self.ctx.name, rc);
                        return Err(RdmaTransportError::OpFailedEx {
                            source: io::Error::last_os_error(),
                            message: err_msg,
                        });
                    }
                    assert!(!cm_event.is_null());
                    cm_event
                };
                let e_type;
                e_type = (unsafe { *cm_event_ptr }).event;

                debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                match e_type {
                    rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_REQUEST => {
                        let mut sendable_cm_event = unsafe {
                            Arc::from(SendableCmEvent(cm_event_ptr))
                        };
                        let client_connection_signal = Arc::new(AtomicBool::new(true));
                        let capsule_context = Arc::from(CapsuleContext::new(MAX_WR as u16).unwrap());
                        self.client_thread_signal.insert(format!("Client-{}", client_number), Arc::clone(&client_connection_signal));
                        let base_dma_handler = self.buffer_manager.get_base_dma();
                        let (rdma_spsc_producer, rdma_spsc_consumer): (Producer<RDMAWorkRequest>, Consumer<RDMAWorkRequest>) = make(1024);
                        let (nvme_spsc_producer, nvme_spsc_consumer): (Producer<InternalNVMeCommandType>, Consumer<InternalNVMeCommandType>) = make(1024);
                        let is_nvme_thread_ready = Arc::new(AtomicBool::new(false));

                        let rdma_thread_handle = {
                            let thread_signal = client_connection_signal.clone();
                            let thread_buffer_manager = self.buffer_manager.clone();
                            let capsule_context_clone = capsule_context.clone();
                            let rdma_event_channel_clone = self.ctx.cm_event_channel.clone();
                            let sendable_cm_event_clone = sendable_cm_event.clone();
                            let block_size = self.block_size.clone();
                            let is_nvme_thread_ready = is_nvme_thread_ready.clone();

                            thread::Builder::new()
                                .name("RDMA Thread".into())
                                .spawn(move || {
                                    Self::_run_rdma_thread(
                                        thread_signal,
                                        thread_buffer_manager,
                                        block_size,
                                        rdma_spsc_consumer,
                                        nvme_spsc_producer,
                                        capsule_context_clone,
                                        sendable_cm_event_clone,
                                        rdma_event_channel_clone,
                                        is_nvme_thread_ready
                                    ).expect(format!("PANIC: handling RDMA thread {}", client_number.to_string()).as_str())
                                }).expect(format!("PANIC: handling RDMA thread {}", client_number.to_string()).as_str())
                        };

                        let nvme_device_thread_handle = {
                            let thread_signal = client_connection_signal.clone();
                            let nvme_device_arc_clone = self.nvme_device_arc.clone();
                            let capsule_context_clone = capsule_context.clone();
                            let mut buffer_lkey;
                            let is_nvme_thread_ready = is_nvme_thread_ready.clone();

                            loop {
                                match self.buffer_manager.get_lkey() {
                                    None => {
                                        continue;
                                    }
                                    Some(lkey) => {
                                        buffer_lkey = lkey;
                                        break;
                                    }
                                }
                            }

                            thread::Builder::new()
                                .name("NVMe Device thread".to_string())
                                .spawn(move || {
                                    let nvme_queue_pair = {
                                        let mut device = nvme_device_arc_clone.lock().unwrap();
                                        device.create_io_queue_pair(QUEUE_LENGTH)
                                    }.map_err(|_| {
                                        RdmaTransportError::OpFailed("Failed to create NMVe Device Queue Pair".into())
                                    }).unwrap();

                                    RdmaTarget::_run_nvme_device_thread(
                                        nvme_queue_pair,
                                        base_dma_handler,
                                        thread_signal,
                                        capsule_context_clone,
                                        buffer_lkey,
                                        rdma_spsc_producer,
                                        nvme_spsc_consumer,
                                        is_nvme_thread_ready
                                    ).expect(format!("PANIC: handling NVMe Device thread {}", client_number.to_string()).as_str());
                                }).expect("Failed to run NVMe Device thread")
                        };

                        client_number = client_number + 1;
                        self.client_handlers.push((rdma_thread_handle, nvme_device_thread_handle));
                    },
                    rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED => {
                        if (unsafe { *cm_event_ptr }).id.is_null() {
                            err_msg = format!("{}: cm_event.id is null.", self.ctx.name);
                            return Err(RdmaTransportError::OpFailedEx {
                                source: io::Error::last_os_error(),
                                message: err_msg
                            });
                        }
                        let cm_id_raw = unsafe { (*cm_event_ptr).id };
                        let address_id = Self::_get_client_address(cm_id_raw);
                        debug_println!("Got rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED event from {}.", address_id);
                        if let Some(signal) = self.client_thread_signal.get_mut(&address_id) {
                            (*signal).store(false, Ordering::SeqCst);
                        }

                        unsafe {
                            let rc = rdma_binding::rdma_ack_cm_event(cm_event_ptr); // Ack the RDMA_CM_EVENT_DISCONNECTED event
                            if rc != 0 {
                                let err_msg = format!("Failed to retrieve a cm event: {}", rc);
                                return Err(RdmaTransportError::OpFailedEx {
                                    source: std::io::Error::last_os_error(),
                                    message: err_msg
                                })
                            }
                        }
                        debug_println!("Stop signal has been sent into the thread {}", address_id);
                        return Ok((0)); // TODO: Just for benchmark. Need to delete this
                    }
                    _ => continue
                }
            }

            for (rdma_thread_handle, nvme_thread_handle) in self.client_handlers {
                rdma_thread_handle.join().unwrap();
                nvme_thread_handle.join().unwrap();
            }

            Ok(0)
        }

        fn _get_client_address(id: *mut rdma_binding::rdma_cm_id) -> String {
            unsafe {
                // Access the src_sin field of the union
                let src_sin = (*id).route.addr.__bindgen_anon_1.src_sin;

                // Extract the IP and port
                let ip = Ipv4Addr::from(u32::from_be(src_sin.sin_addr.s_addr));
                let port = u16::from_be(src_sin.sin_port);

                format!("{}:{}", ip, port)
            }
        }

        fn _run_rdma_thread(
            running_signal: Arc<AtomicBool>,
            mut buffer_manager: Arc<BufferManager>,
            block_size: usize,
            mut rdma_spsc_consumer: Consumer<RDMAWorkRequest>,
            mut nvme_spsc_producer: Producer<InternalNVMeCommandType>,
            mut capsule_context: Arc<CapsuleContext>,
            cm_event: Arc<SendableCmEvent>,
            rdma_event_channel: Arc<RdmaEventChannelPtr>,
            is_nvme_thread_ready: Arc<AtomicBool>,
        ) -> Result<(), RdmaTransportError> {
            #[cfg(enable_trace)]
            let thread_span = span!(Level::INFO, "RDMA Thread");
            #[cfg(enable_trace)]
            let _thread_span = thread_span.enter();
            let mut client_context;
            debug_println_verbose!("Received RDMA_CM_EVENT_CONNECT_REQUEST...");

            let cm_id_ptr = unsafe {
                debug_println_verbose!("getting cm_id");
                let cm_id_ptr =(*cm_event.0).id;
                if cm_id_ptr.is_null() {
                    return Err(RdmaTransportError::OpFailed("Failed to get cm_id".into()));
                }
                debug_println_verbose!("cm_id is ok");

                cm_id_ptr
            };

            let mut pd_ptr;
            unsafe {
                pd_ptr = rdma_binding::ibv_alloc_pd((*cm_id_ptr).verbs);
                if pd_ptr.is_null() {
                    return Err(RdmaTransportError::FailedResourceInit("protection domain".parse().unwrap()))
                }
            }

            buffer_manager.register_mr(pd_ptr)?;

            // ACK the event. rdma_ack_cm_event frees the cm_event object, but not object inside of it.
            unsafe {
                debug_println_verbose!("ack cm_event");
                let rc = rdma_binding::rdma_ack_cm_event(cm_event.0); // Ack the RDMA_CM_EVENT_CONNECT_REQUEST event
                if rc != 0 {
                    let err_msg = format!("Failed to retrieve a cm event: {}", rc);
                    return Err(RdmaTransportError::OpFailedEx {
                        source: std::io::Error::last_os_error(),
                        message: err_msg
                    })
                }
                debug_println_verbose!("ack cm_event: success");
            }

            // Now we accept the connection. Recall we have not accepted the connection
            // yet because we have to do lots of resource pre-allocation
            let mut conn_param: rdma_binding::rdma_conn_param = unsafe { mem::zeroed() };
            // this tell how many outstanding requests can we handle
            conn_param.initiator_depth = u8::MAX;
            // This tell how many outstanding requests we expect other side to handle
            conn_param.responder_resources = u8::MAX;
            client_context = ClientRdmaContext::new(cm_id_ptr, pd_ptr, MAX_WR as u16)?;

            capsule_context.register_mr(client_context.pd).expect("PANIC: Failed to register capsule MR");
            let mut rdma_work_manager = Arc::from(RdmaWorkManager::new(MAX_WR as u16));
            debug_println_verbose!("Handling client thread start.");
            let qp = client_context.get_sendable_qp();
            let cq = client_context.get_sendable_cq();
            // Initially post recv WR. Saturate the queue.
            debug_println_verbose!("Pre-Posting rcv work");
            while let Some(wr_id) = rdma_work_manager.allocate_wr_id() {
                let sge = capsule_context.get_req_sge(wr_id as usize).unwrap();
                rdma_work_manager.post_rcv_req_work(wr_id, &qp, sge).expect("PANIC: failed to post RDMA recv.");
            }

            unsafe {
                while !is_nvme_thread_ready.load(Ordering::SeqCst) {}
                debug_println_verbose!("accept connection");
                let rc = rdma_binding::rdma_accept(client_context.cm_id, &mut conn_param);

                if rc != 0 {
                    let err_msg = format!("{}: Failed to accept the connection. {}", client_context._name, rc);
                    return Err(RdmaTransportError::OpFailedEx {
                        source: std::io::Error::last_os_error(),
                        message: err_msg
                    })
                }
                debug_println_verbose!("accept connection: success");
            }

            debug_println_verbose!("waiting for : RDMA_CM_EVENT_ESTABLISHED event...");
            let mut client_name;

            unsafe {
                let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
                let rc = process_cm_event(rdma_event_channel.0, &mut cm_event)?;
                if rc != 0 {
                    let err_msg = format!("{}: Failed to get cm event: {}", client_context._name, rc);
                    return Err(RdmaTransportError::OpFailedEx {
                        source: io::Error::last_os_error(),
                        message: err_msg
                    });
                }

                let e_type = (*cm_event).event;
                debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                if e_type != rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED {
                    let err_msg = format!("{}: Received even with type RDMA_CM_EVENT_ESTABLISHED. Got {} instead.", client_context._name, e_type);
                    return Err(RdmaTransportError::OpFailedEx {
                        source: io::Error::last_os_error(),
                        message: err_msg
                    });
                }

                client_name = unsafe {
                    let id = (*cm_event).id;
                    Self::_get_client_address(id)
                };

                rdma_binding::rdma_ack_cm_event(cm_event);

                if rc != 0 {
                    let err_msg = format!("{}: Failed to acknowledge the cm event: {}", client_context._name, rc);
                    return Err(RdmaTransportError::OpFailedEx {
                        source: io::Error::last_os_error(),
                        message: err_msg
                    });
                }
            }

            // now, run the thread loop
            let mut any_inflight_wr = rdma_work_manager.any_inflight_wr();

            while running_signal.load(Ordering::SeqCst) || any_inflight_wr {
                while let Some(rdma_wr) = rdma_spsc_consumer.try_pop() {
                    let mode = rdma_wr.mode.unwrap();
                    match mode {
                        rdma_binding::ibv_wr_opcode_IBV_WR_SEND => {
                            debug_println_verbose!("[RDMA SUBMISSION THREAD] post_send_response_work wr_id={}", rdma_wr.wr_id);
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "Post RDMA SEND");
                            #[cfg(enable_trace)]
                            let _s = s.enter();
                            rdma_work_manager.post_send_response_work(rdma_wr.wr_id, &qp, rdma_wr.sge).unwrap();
                        },
                        rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE => {
                            debug_println_verbose!("[RDMA SUBMISSION THREAD] post_rmt_work WRITE wr_id={}", rdma_wr.wr_id);
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "Post RDMA WRITE");
                            #[cfg(enable_trace)]
                            let _s = s.enter();
                            rdma_work_manager.post_rmt_work(
                                rdma_wr.wr_id,
                                &qp,
                                rdma_wr.sge,
                                rdma_wr.remote_info.unwrap().0,
                                rdma_wr.remote_info.unwrap().1,
                                rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE
                            ).expect("Panic: failed to post remote work");
                        },
                        _ => {}
                    }
                }

                // debug_println!("[RDMA COMPLETION THREAD] Polling RDMA completion....");
                #[cfg(enable_trace)]
                let poll_span = span!(Level::INFO, "Poll RDMA WC");
                #[cfg(enable_trace)]
                let poll_guard = poll_span.entered();
                let any_completion = rdma_work_manager.try_poll_completed_works(&cq).unwrap();
                #[cfg(enable_trace)]
                drop(poll_guard);

                if !any_completion {
                    spin_loop();
                    continue;
                }

                while let Some(wc) = rdma_work_manager.next_wc() {
                    #[cfg(enable_trace)]
                    let outer_loop_span = span!(Level::INFO, "RDMA WC processing");
                    #[cfg(enable_trace)]
                    let _outer_loop_span = outer_loop_span.enter();
                    let completed_wr_id = wc.wr_id;
                    let op_code = wc.opcode;
                    let op_code_str = match op_code {
                        rdma_binding::ibv_wc_opcode_IBV_WC_RECV => "IBV_WC_RECV",
                        rdma_binding::ibv_wc_opcode_IBV_WC_SEND => "IBV_WC_SEND",
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_READ => {
                            "IBV_WC_RDMA_READ"
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_WRITE => {
                            "IBV_WC_RDMA_WRITE"
                        }
                        _ => "unexpected op code",
                    };
                    let wc_status = wc.status;
                    #[cfg(debug_mode_verbose)]
                    {
                        let status_str = unsafe {
                            CStr::from_ptr(rdma_binding::ibv_wc_status_str(wc_status))
                                .to_string_lossy() // converts to Cow<str>, handles invalid UTF-8 safely
                                .into_owned()
                        };

                        if status_str !=  "Work Request Flushed Error" {
                            println!(
                                "[RDMA COMPLETION THREAD] Got a completion wr_id: {}, op_code: {}, status: {}",
                                completed_wr_id,
                                op_code_str,
                                status_str
                            );
                        }
                    }

                    // #[cfg(enable_trace)]
                    // let span = span!(Level::INFO, "check running_signal");
                    // let guard = span.entered();
                    // if !running_signal.load(Ordering::SeqCst) {
                    //     rdma_work_manager.release_wr(completed_wr_id as u16).map_err(|_| { RdmaTransportError::OpFailed("failed to release WR".into()) })?;
                    //     continue;
                    // }
                    // #[cfg(enable_trace)]
                    // drop(guard);

                    if wc_status != rdma_binding::ibv_wc_status_IBV_WC_SUCCESS {
                        #[cfg(enable_trace)]
                        let s = span!(Level::INFO, "On not IBV_WC_SUCCESS");
                        #[cfg(enable_trace)]
                        let _s = s.enter();
                        debug_println!("[RDMA COMPLETION THREAD] Releasing wr_id after failed RDMA WC....");
                        rdma_work_manager.release_wr(completed_wr_id as u16).map_err(|_| {
                            RdmaTransportError::OpFailed("failed to release WR".into())
                        })?;

                        if op_code == rdma_binding::ibv_wc_opcode_IBV_WC_RECV {
                            // client might be disconnected
                            if wc_status == rdma_binding::ibv_wc_status_IBV_WC_WR_FLUSH_ERR {
                                running_signal.store(false, Ordering::SeqCst);
                            }
                            continue;
                        }
                        eprintln!(
                            "[NOT IBV_WC_SUCCESS] wr_id: {}, op_code: {}, status: {}",
                            completed_wr_id, op_code_str, wc_status
                        );
                    }

                    match op_code {
                        rdma_binding::ibv_wc_opcode_IBV_WC_RECV => {
                            // we got the capsule.
                            // process read/write accordingly
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "On IBV_WC_RECV");
                            #[cfg(enable_trace)]
                            let _s = s.enter();

                            let (cmd, lba, virtual_addr, data_len, r_key) = capsule_context
                                .get_request_capsule_content(completed_wr_id as usize)
                                .unwrap();

                            debug_println!(
                                "[RDMA COMPLETION THREAD] received I/O request. cmd_opcode: {}, len: {}",
                                cmd.opcode,
                                data_len
                            );


                            let ((buffer_virtual_add, _, _, _), buffer_idx, lkey) = {
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "buffer_manager.allocate()");
                                #[cfg(enable_trace)]
                                let _s = s.enter();
                                let (buffer_idx, mr) = buffer_manager.allocate().expect("Failed to allocate buffer from buffer manager"); // TODO(what should we do when there is no available buffer?)
                                let lkey = unsafe {
                                    assert!(!mr.is_null(), "Buffer manager MR is null");
                                    (*mr).lkey.clone()
                                };
                                client_context.set_wr_id_buffer_idx(completed_wr_id as usize, buffer_idx);
                                (buffer_manager.get_memory_info(buffer_idx), buffer_idx, lkey)
                            };

                            match cmd.opcode {
                                1 => { // NVMe write: RDMA remote read -> NVMe write -> RDMA send response
                                    #[cfg(enable_trace)]
                                    let s = span!(Level::INFO, "On IBV_WC_RECV post RDMA_READ work");
                                    #[cfg(enable_trace)]
                                    let _s = s.enter();
                                    rdma_work_manager.post_rmt_work(
                                        completed_wr_id as u16,
                                        &qp,
                                        rdma_binding::ibv_sge {
                                            addr: buffer_virtual_add as u64,
                                            length: data_len,
                                            lkey,
                                        },
                                        virtual_addr,
                                        r_key,
                                        rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_READ
                                    ).expect("Panic: failed to post remote work");
                                },
                                2 => { // NVMe read: NVMe read -> RDMA remote write -> RDMA send response
                                    #[cfg(enable_trace)]
                                    let s = span!(Level::INFO, "On IBV_WC_RECV: notify NVME thread to READ");
                                    #[cfg(enable_trace)]
                                    let _s = s.enter();
                                    nvme_spsc_producer.push((completed_wr_id as u16, buffer_idx as usize * block_size, lba, false, data_len as usize));
                                },
                                _ => {}
                            }
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_SEND => {
                            // means response capsule is sent. Release all resources.
                            {
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "On (response capsule) IBV_WC_SEND: release resource");
                                #[cfg(enable_trace)]
                                let _s = s.enter();
                                debug_println!("[RDMA COMPLETION THREAD] Response is sent for wr_id: {}", completed_wr_id);
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "get_remote_op_buffer(completed_wr_id)");
                                #[cfg(enable_trace)]
                                let guard = s.entered();
                                let buffer_idx = client_context.get_remote_op_buffer(completed_wr_id as usize)?;
                                #[cfg(enable_trace)]
                                drop(guard);
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, " bm.free(buffer_idx)");
                                #[cfg(enable_trace)]
                                let guard = s.entered();
                                buffer_manager.free(buffer_idx);
                                #[cfg(enable_trace)]
                                drop(guard);
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "ctx.free_remote_op_buffer(wr_id)");
                                #[cfg(enable_trace)]
                                let guard = s.entered();
                                client_context.free_remote_op_buffer(completed_wr_id as usize)?;
                                #[cfg(enable_trace)]
                                drop(guard);
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "rdma_work_manager.release_wr(wr_id)");
                                #[cfg(enable_trace)]
                                let guard = s.entered();
                                rdma_work_manager.release_wr(completed_wr_id as u16).unwrap();
                                #[cfg(enable_trace)]
                                drop(guard);
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "rwm.allocate_wr_id()");
                                #[cfg(enable_trace)]
                                let guard = s.entered();
                                let new_wr_id = rdma_work_manager.allocate_wr_id().unwrap();
                                let sge = capsule_context.get_req_sge(new_wr_id as usize).unwrap();
                                #[cfg(enable_trace)]
                                drop(guard);
                                debug_println_verbose!("[RDMA COMPLETION THREAD] Posting another receive request with wr_id={}", completed_wr_id);
                                #[cfg(enable_trace)]
                                let s = span!(Level::INFO, "rwm.post_rcv_req_work(new_wr_id, qp, sge)");
                                #[cfg(enable_trace)]
                                let guard = s.entered();
                                rdma_work_manager.post_rcv_req_work(new_wr_id, &qp, sge).unwrap();
                                #[cfg(enable_trace)]
                                drop(guard);
                            }
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_READ => {
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "On IBV_WC_RDMA_READ: notify NVMe thread to WRITE");
                            #[cfg(enable_trace)]
                            let _s = s.enter();
                            // This is a write I/O. Hence, call the send_nvme_io_write()
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, " client_context.get_remote_op_buffer(wr_id)");
                            #[cfg(enable_trace)]
                            let guard = s.entered();
                            let buffer_idx = client_context.get_remote_op_buffer(completed_wr_id as usize)?;
                            #[cfg(enable_trace)]
                            drop(guard);
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, " capsule_context.get_request_capsule_content(wr_id)");
                            #[cfg(enable_trace)]
                            let guard = s.entered();
                            let (_cmd, lba, _virtual_addr, data_len, _r_key) = capsule_context
                                .get_request_capsule_content(completed_wr_id as usize)
                                .unwrap();
                            #[cfg(enable_trace)]
                            drop(guard);
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "nvme_spsc_producer.push()");
                            #[cfg(enable_trace)]
                            let guard = s.entered();
                            nvme_spsc_producer.push((completed_wr_id as u16, buffer_idx as usize * block_size, lba, true, data_len as usize));
                            #[cfg(enable_trace)]
                            drop(guard);
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_WRITE => {
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "On IBV_WC_RDMA_WRITE: send response");
                            #[cfg(enable_trace)]
                            let _s = s.enter();

                            // This is a read I/O. This means the final remote write has been completed. Then, send response
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "write response + ccapsule_context.get_resp_sge(wr_id)");
                            #[cfg(enable_trace)]
                            let guard = s.entered();
                            capsule_context.set_response_status(completed_wr_id as usize, 0).unwrap();
                            let resp_sge = capsule_context.get_resp_sge(completed_wr_id as usize)
                                .unwrap();
                            #[cfg(enable_trace)]
                            drop(guard);
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "rwm.post_send_response_work()");
                            #[cfg(enable_trace)]
                            let guard = s.entered();
                            rdma_work_manager.post_send_response_work(completed_wr_id as u16, &qp, resp_sge).unwrap();
                            #[cfg(enable_trace)]
                            drop(guard);
                        }
                        _ => {}
                    }
                }

                any_inflight_wr = rdma_work_manager.any_inflight_wr();
            }

            Ok(())
        }

        fn _run_nvme_device_thread(
            mut nvme_queue_pair: NvmeQueuePair,
            base_dma: ThreadSafeDmaHandle,
            running_signal: Arc<AtomicBool>,
            mut capsule_context: Arc<CapsuleContext>,
            buffer_l_key: u32,
            mut rdma_spsc_producer: Producer<RDMAWorkRequest>,
            mut nvme_spsc_consumer: Consumer<InternalNVMeCommandType>,
            is_nvme_thread_ready: Arc<AtomicBool>,
        ) -> Result<i32, RdmaTransportError> {
            #[cfg(enable_trace)]
            let s = span!(Level::INFO, "NVMe Thread");
            #[cfg(enable_trace)]
            let _s = s.enter();
            let mut c_id_to_offset_map: Vec<Option<usize>> = vec![None; QUEUE_LENGTH];
            let mut empty_result_cnt = 0usize;
            is_nvme_thread_ready.store(true, Ordering::SeqCst);

            while running_signal.load(Ordering::SeqCst) {
                let mut current_command;
                #[cfg(enable_trace)]
                let guard = span!(Level::INFO, "SPSC pop").entered();
                while let Some(command) = nvme_spsc_consumer.try_pop() {
                    #[cfg(enable_trace)]
                    let s = span!(Level::INFO, "Submit NVMe I/O");
                    #[cfg(enable_trace)]
                    let _s = s.enter();
                    empty_result_cnt = 0;
                    current_command = command;
                    let (c_id, start_offset, lba, write, size) = current_command;
                    c_id_to_offset_map[c_id as usize] = Some(start_offset);

                    unsafe {
                        debug_println_verbose!("[NVMe Device Thread] Submit I/O {} command: bytes_offset: {}, lba: {}, size: {}", if write { "WRITE" } else { "READ" }, start_offset, lba, size);
                        nvme_queue_pair.submit_io_with_cid(
                            &base_dma.to_dma().slice(start_offset..start_offset + size),
                            lba,
                            write,
                            c_id
                        );
                    }
                }
                #[cfg(enable_trace)]
                drop(guard);
                #[cfg(enable_trace)]
                let guard = span!(Level::INFO, "nvme_qp.poll_completion()").entered();

                while let Some(completion) = nvme_queue_pair.quick_poll_completion() {
                    #[cfg(enable_trace)]
                    let s = span!(Level::INFO, "On NVMe Completion found");
                    #[cfg(enable_trace)]
                    let _s = s.enter();
                    empty_result_cnt = 0; // if find any, reset the counter
                    debug_println!("[NVMe Device Thread] I/O is completed. cid = {}, status = {}", completion.c_id as u16, (completion.status >> 1) as u16);
                    let wr_id = completion.c_id & 0x7FF;

                    let (opcode, virtual_addr, r_key, data_len, resp_sge) = {
                        let (cmd, lba, virtual_addr, data_len, r_key) = capsule_context
                            .get_request_capsule_content(wr_id as usize)
                            .unwrap();
                        let resp_sge = capsule_context.get_resp_sge(wr_id as usize).unwrap();
                        (cmd.opcode.clone(), virtual_addr, r_key, data_len, resp_sge)
                    };

                    match opcode {
                        1 => { // NVMe write is completed -> send response
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "On NVMe WRITE Completion found");
                            #[cfg(enable_trace)]
                            let _s = s.enter();
                            debug_println_verbose!("[NVMe Device Thread] Found NVMe device write I/O completion ....");
                            {
                                let mut capsule = capsule_context.get_resp_capsule(wr_id as usize).unwrap();
                                capsule.status = (completion.status >> 1) as i16;
                                capsule.cmd_id = wr_id;
                            }

                            rdma_spsc_producer.push(RDMAWorkRequest {
                                wr_id,
                                sge: resp_sge,
                                mode: Some(rdma_binding::ibv_wr_opcode_IBV_WR_SEND),
                                remote_info: None,
                            });
                        },
                        2 => { // NVMe read is completed -> post RDMA remote write
                            #[cfg(enable_trace)]
                            let s = span!(Level::INFO, "On NVMe READ Completion found");
                            #[cfg(enable_trace)]
                            let _s = s.enter();
                            let offset = c_id_to_offset_map[wr_id as usize].unwrap();
                            let mut local_sge = unsafe {
                                rdma_binding::ibv_sge {
                                    addr: base_dma.virt.add(offset) as u64,
                                    length: data_len,
                                    lkey: buffer_l_key,
                                }
                            };

                            rdma_spsc_producer.push(RDMAWorkRequest {
                                wr_id,
                                sge: local_sge,
                                mode: Some(rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE),
                                remote_info: Some((virtual_addr, r_key)),
                            });
                            c_id_to_offset_map[wr_id as usize] = None;
                        },
                        _ => {}
                    }
                }
                #[cfg(enable_trace)]
                drop(guard);

                // empty_result_cnt = empty_result_cnt + 1;
                //
                // match empty_result_cnt {
                //     0..=1000 => {
                //         // do nothing == spin loop
                //         spin_loop(); // hint CPU
                //     },
                //     1001..=2500 => {
                //         // yield
                //         thread::yield_now();
                //     },
                //     2501..=2510 => {
                //         // micro-seconds sleep (make sure 1/4 of expected NVMe I/O latency
                //         thread::sleep(Duration::from_micros(5))
                //     },
                //     2511..=3000 => {
                //         // Accommodate big block I/O
                //         thread::sleep(Duration::from_micros(100))
                //     },
                //     3001..=3060 => {
                //         // Accommodate big block I/O
                //         debug_println_verbose!("[NVMe Device Thread] arrived {} steps without new submission. Sleeping for 1 seconds", empty_result_cnt);
                //         thread::sleep(Duration::from_secs(1))
                //     },
                //     _ => {
                //         // do nothing, we will block in the next step
                //     }
                // }
            }

            Ok(0)
        }
    }
}

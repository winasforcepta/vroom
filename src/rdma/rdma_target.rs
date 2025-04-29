pub mod rdma_target {
    use crate::rdma::buffer_manager::{BufferManager, ThreadSafeDmaHandle};
    use crate::rdma::rdma_common::rdma_binding;
    use crate::rdma::rdma_common::rdma_common::{get_rdma_event_type_string, process_cm_event, ClientRdmaContext, RdmaTransportError, MAX_WR};
    use crate::rdma::rdma_common::*;
    use crate::rdma::rdma_work_manager::rdma_work_manager::{post_rmt_work, post_send_response_work, RdmaWorkManager};
    use crate::{NvmeDevice, NvmeQueuePair, QUEUE_LENGTH};
    use std::collections::{HashMap};
    use std::net::Ipv4Addr;
    use std::ptr::null_mut;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::{io, mem, ptr, thread};
    use std::ffi::CStr;
    use std::time::Duration;
    use crossbeam::channel::{bounded, Receiver, Sender};
    use crate::memory::DmaSlice;
    use crate::rdma::capsule::capsule::{CapsuleContext};

    type InternalNVMeCommandType = (
        u16, // cid
        usize, // start offset
        u64, // nvme LBA
        bool, // is write
        usize // data len
    );

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
        cm_event_channel_arc: Arc<Mutex<RdmaEventChannelPtr>>,
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
                cm_event_channel_arc: Arc::from(Mutex::from(RdmaEventChannelPtr(cm_event_channel))),
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
                let cm_event_channel = self.cm_event_channel_arc.lock().unwrap().0;
                rdma_binding::rdma_destroy_event_channel(cm_event_channel);
            }
        }
    }

    pub struct RdmaTarget {
        server_sockaddr: rdma_binding::sockaddr_in,
        ctx: TargetRdmaContext,
        client_handlers: Vec<(JoinHandle<()>, JoinHandle<()>, JoinHandle<()>)>,
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

            // handle
            loop {
                let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
                {
                    let cm_event_channel = self.ctx.cm_event_channel_arc.lock().unwrap().0;
                    let rc = process_cm_event(cm_event_channel, &mut cm_event)?;
                    if rc != 0 {
                        err_msg = format!("{}: Failed to get cm event: {}", self.ctx.name, rc);
                        return Err(RdmaTransportError::OpFailedEx {
                            source: io::Error::last_os_error(),
                            message: err_msg,
                        });
                    }
                }


                if cm_event.is_null() {
                    err_msg = format!(
                        "{}: Returned cm_event pointer is null after calling _process_event.",
                        self.ctx.name
                    );
                    return Err(RdmaTransportError::OpFailedEx {
                        source: io::Error::last_os_error(),
                        message: err_msg,
                    });
                }

                let e_type;
                e_type = (unsafe { *cm_event }).event;

                debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                match e_type {
                    rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_REQUEST => {
                        debug_println_verbose!("Received RDMA_CM_EVENT_CONNECT_REQUEST...");
                        let cm_id_ptr;

                        unsafe {
                            debug_println_verbose!("getting cm_id");
                            cm_id_ptr = (*cm_event).id;
                            if cm_id_ptr.is_null() {
                                return Err(RdmaTransportError::OpFailed("Failed to get cm_id".into()));
                            }
                            debug_println_verbose!("cm_id is ok");
                        }

                        let mut pd_ptr;
                        unsafe {
                            pd_ptr = rdma_binding::ibv_alloc_pd((*cm_id_ptr).verbs);
                            if pd_ptr.is_null() {
                                return Err(RdmaTransportError::FailedResourceInit("protection domain".parse().unwrap()))
                            }
                        }

                        self.buffer_manager.register_mr(pd_ptr)?;

                        // ACK the event. rdma_ack_cm_event frees the cm_event object, but not object inside of it.
                        unsafe {
                            debug_println_verbose!("ack cm_event");
                            let rc = rdma_binding::rdma_ack_cm_event(cm_event); // Ack the RDMA_CM_EVENT_CONNECT_REQUEST event
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
                        let mut capsule_context = Arc::from(CapsuleContext::new(pd_ptr, MAX_WR as u16).unwrap());
                        let mut client_context = Arc::from(ClientRdmaContext::new(cm_id_ptr, pd_ptr, MAX_WR as u16)?);

                        unsafe {
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

                        unsafe {
                            let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
                            let server_event_channel = self.ctx.cm_event_channel_arc.lock().unwrap().0;
                            let rc = process_cm_event(server_event_channel, &mut cm_event)?;
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

                            rdma_binding::rdma_ack_cm_event(cm_event);

                            if rc != 0 {
                                let err_msg = format!("{}: Failed to acknowledge the cm event: {}", client_context._name, rc);
                                return Err(RdmaTransportError::OpFailedEx {
                                    source: io::Error::last_os_error(),
                                    message: err_msg
                                });
                            }
                        }

                        let mut rwm_mutex = Arc::from(Mutex::from(RdmaWorkManager::new(MAX_WR as u16)));
                        debug_println_verbose!("Handling client thread start.");
                        // Initially post recv WR. Saturate the queue.
                        debug_println_verbose!("Posting rcv work");
                        {
                            let mut rwm = rwm_mutex.lock().unwrap();
                            while let Some(wr_id) = rwm.allocate_wr_id() {
                                let sge = capsule_context.get_req_sge(wr_id as usize).unwrap();
                                let qp = client_context.get_sendable_qp();
                                rwm.post_rcv_req_work(wr_id, qp, sge).expect("PANIC: failed to post RDMA recv.");
                            }
                        }
                        debug_println_verbose!("Posting rcv work: success");
                        debug_println!("A new connection is accepted.");
                        let signal = Arc::new(AtomicBool::new(true));
                        let client_name = unsafe {
                            let id = (*cm_event).id;
                            Self::_get_client_address(id)
                        };
                        self.client_thread_signal.insert(client_name, Arc::clone(&signal));
                        let base_dma_handler = self.buffer_manager.get_base_dma();
                        let thread_signal = signal.clone();
                        let thread_buffer_manager = self.buffer_manager.clone();
                        let signal_clone_2 = thread_signal.clone();
                        let block_size = self.block_size.clone();
                        let (nvme_command_sx, nvme_command_rx) = bounded::<InternalNVMeCommandType>(1024);
                        let (rdma_wr_sx, rdma_wr_rx) = bounded::<RDMAWorkRequest>(1024);
                        let rdma_completion_thread_handle = {
                            let rwm_mutex_clone = rwm_mutex.clone();
                            let signal_clone = thread_signal.clone();
                            let capsule_context_clone = capsule_context.clone();
                            let rdma_wr_sx_clone = rdma_wr_sx.clone();
                            let client_context_clone = client_context.clone();

                            thread::Builder::new()
                                .name("RDMA Completion thread".to_string())
                                .spawn(move || {
                                    RdmaTarget::_run_rdma_completion_thread(
                                        signal_clone,
                                        thread_buffer_manager,
                                        block_size,
                                        client_context_clone,
                                        capsule_context_clone,
                                        nvme_command_sx,
                                        rdma_wr_sx_clone,
                                        rwm_mutex_clone
                                    ).expect(format!("PANIC: handling RDMA thread {}", client_number.to_string()).as_str());
                                }).expect("Failed to run RDMA thread")
                        };
                        let nvme_device_arc_clone = self.nvme_device_arc.clone();
                        let buffer_lkey = self.buffer_manager.get_lkey().unwrap();
                        let rdma_submission_thread_handle = {
                            let rwm_mutex_clone = rwm_mutex.clone();
                            let rdma_wr_rx_clone = rdma_wr_rx.clone();
                            let client_context_clone = client_context.clone();

                            thread::Builder::new()
                                .name("RDMA Submission thread".to_string())
                                .spawn(move || {
                                    RdmaTarget::_run_rdma_submission_thread(
                                        client_context_clone,
                                        rwm_mutex_clone,
                                        rdma_wr_rx_clone
                                    ).expect(format!("PANIC: handling RDMA submission thread {}", client_number.to_string()).as_str());
                                }).expect("Failed to run RDMA submission thread")
                        };
                        let nvme_device_thread_handle = {
                            let rdma_wr_sx_clone = rdma_wr_sx.clone();
                            let capsule_context_clone = capsule_context.clone();

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
                                        signal_clone_2,
                                        capsule_context_clone,
                                        buffer_lkey,
                                        nvme_command_rx,
                                        rdma_wr_sx_clone
                                    ).expect(format!("PANIC: handling NVMe Device thread {}", client_number.to_string()).as_str());
                                }).expect("Failed to run NVMe Device thread")
                        };

                        client_number = client_number + 1;
                        self.client_handlers.push((rdma_completion_thread_handle, rdma_submission_thread_handle, nvme_device_thread_handle));
                    },
                    rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED => {
                        if (unsafe { *cm_event }).id.is_null() {
                            err_msg = format!("{}: cm_event.id is null.", self.ctx.name);
                            return Err(RdmaTransportError::OpFailedEx {
                                source: io::Error::last_os_error(),
                                message: err_msg
                            });
                        }
                        let cm_id_raw = unsafe { (*cm_event).id };
                        let address_id = Self::_get_client_address(cm_id_raw);
                        debug_println!("Got rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED event from {}.", address_id);
                        if let Some(signal) = self.client_thread_signal.get_mut(&address_id) {
                            (*signal).store(false, Ordering::SeqCst);
                        }

                        unsafe {
                            let rc = rdma_binding::rdma_ack_cm_event(cm_event); // Ack the RDMA_CM_EVENT_DISCONNECTED event
                            if rc != 0 {
                                let err_msg = format!("Failed to retrieve a cm event: {}", rc);
                                return Err(RdmaTransportError::OpFailedEx {
                                    source: std::io::Error::last_os_error(),
                                    message: err_msg
                                })
                            }
                        }
                        debug_println!("Stop signal has been sent into the thread {}", address_id);
                    }
                    _ => return Err(RdmaTransportError::OpFailedEx {
                        source: io::Error::last_os_error(),
                        message: format!("Expecting RDMA_CM_EVENT_CONNECT_REQUEST but got a unexpected event: {}", get_rdma_event_type_string(e_type)),
                    })
                }
            }

            for (rdma_completion_thread_handle, rdma_submission_thread_handle, nvme_thread_handle) in self.client_handlers {
                rdma_completion_thread_handle.join().unwrap();
                rdma_submission_thread_handle.join().unwrap();
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

        // Handle connection request
        // Steps:
        //     - ACK the RDMA_CM_EVENT_CONNECT_REQUEST event
        //     - Setup client resources (at this moment, we only support a single client)
        //     - Accept the connection via rdma_binding::rdma_accept(2)
        //     - Exchange metadata with client
        fn _run_rdma_completion_thread(
            running_signal: Arc<AtomicBool>,
            mut buffer_manager: Arc<BufferManager>,
            block_size: usize,
            mut client_context: Arc<ClientRdmaContext>,
            mut capsule_context: Arc<CapsuleContext>,
            nvme_command_sx: Sender<InternalNVMeCommandType>,
            rdma_wr_sx: Sender<RDMAWorkRequest>,
            rwm_mutex: Arc<Mutex<RdmaWorkManager>>
        ) -> Result<i32, RdmaTransportError> {
            let mut any_inflight_wr = {
                let rwm = rwm_mutex.lock().unwrap();
                rwm.any_inflight_wr()
            };

            while running_signal.load(Ordering::SeqCst) || any_inflight_wr {
                // idea: for every loop:
                //  - poll_completion
                //  - loop over the completed WCs
                //  - post next works accordingly
                debug_println!("Polling RDMA completion....");
                {
                    let mut rwm = rwm_mutex.lock().unwrap();
                    let io_comp_channel = client_context.get_sendable_io_comp_channel();
                    let cq = client_context.get_sendable_cq();
                    rwm.poll_completed_works(io_comp_channel, cq).unwrap();
                };

                loop {
                    let completed_wr_id;
                    let op_code;
                    let op_code_str;
                    let wc_status;

                    {
                        debug_println!("Polling RDMA completion....");
                        let mut rwm = rwm_mutex.lock().unwrap();
                        let wc_opt = rwm.next_wc();

                        match wc_opt {
                            None => {
                                break;
                            }
                            Some(wc) => {
                                completed_wr_id = wc.wr_id;
                                op_code = wc.opcode;
                                op_code_str = match op_code {
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
                                wc_status = wc.status;
                            }
                        }
                    }

                    #[cfg(debug_mode_verbose)]
                    {
                        let status_str = unsafe {
                            CStr::from_ptr(rdma_binding::ibv_wc_status_str(wc_status))
                                .to_string_lossy() // converts to Cow<str>, handles invalid UTF-8 safely
                                .into_owned()
                        };

                        println!(
                            "Got a completion wr_id: {}, op_code: {}, status: {}",
                            completed_wr_id,
                            op_code_str,
                            status_str
                        );
                    }

                    if !running_signal.load(Ordering::SeqCst) {
                        let mut rwm = rwm_mutex.lock().unwrap();
                        rwm.release_wr(completed_wr_id as u16).map_err(|_| { RdmaTransportError::OpFailed("failed to release WR".into()) })?;
                        continue;
                    }

                    if wc_status != rdma_binding::ibv_wc_status_IBV_WC_SUCCESS {
                        debug_println!("Releasing wr_id after failed RDMA WC....");
                        {
                            let mut rwm = rwm_mutex.lock().unwrap();
                            rwm.release_wr(completed_wr_id as u16).map_err(|_| {
                                RdmaTransportError::OpFailed("failed to release WR".into())
                            })?;
                        }
                        
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
                    let qp  = client_context.get_sendable_qp();

                    match op_code {
                        rdma_binding::ibv_wc_opcode_IBV_WC_RECV => {
                            // we got the capsule.
                            // process read/write accordingly
                            let (cmd, lba, virtual_addr, data_len, r_key) = capsule_context
                                .get_request_capsule_content(completed_wr_id as usize)
                                .unwrap();

                            debug_println!(
                                "received I/O request. cmd_opcode: {}, len: {}",
                                cmd.opcode,
                                data_len
                            );

                            let ((buffer_virtual_add, _, _, _), buffer_idx, lkey) = {
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
                                    rdma_wr_sx.send(RDMAWorkRequest {
                                        wr_id: completed_wr_id as u16,
                                        sge: rdma_binding::ibv_sge {
                                            addr: buffer_virtual_add as u64,
                                            length: data_len,
                                            lkey,
                                        },
                                        mode: Some(rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_READ),
                                        remote_info: Some((virtual_addr, r_key))
                                    }).expect("PANIC: Failed to send IBV_WR_RDMA_READ command to the RDMA submission thread.");
                                },
                                2 => { // NVMe read: NVMe read -> RDMA remote write -> RDMA send response
                                    nvme_command_sx.send((completed_wr_id as u16, buffer_idx as usize * block_size, lba, false, data_len as usize)).unwrap();
                                },
                                _ => {}
                            }
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_SEND => {
                            // means response capsule is sent. Release all resources.
                            {
                                debug_println!("Response is sent for wr_id: {}", completed_wr_id);
                                let buffer_idx = client_context.get_remote_op_buffer(completed_wr_id as usize)?;
                                buffer_manager.free(buffer_idx);
                                client_context.free_remote_op_buffer(completed_wr_id as usize)?;
                                let new_wr_id = {
                                    let mut rwm = rwm_mutex.lock().unwrap();
                                    rwm.release_wr(completed_wr_id as u16).unwrap();
                                    rwm.allocate_wr_id().unwrap()
                                };
                                let sge = capsule_context.get_req_sge(new_wr_id as usize).unwrap();
                                debug_println_verbose!("Posting another receive request with wr_id={}", completed_wr_id);
                                rdma_wr_sx.send(RDMAWorkRequest {
                                    wr_id: new_wr_id,
                                    sge,
                                    mode: None,
                                    remote_info: None
                                }).expect("PANIC: Failed to send IBV_WR_RDMA_RECV command to the RDMA submission thread.");
                                
                            }
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_READ => {
                            // This is a write I/O. Hence, call the send_nvme_io_write()
                            let buffer_idx = client_context.get_remote_op_buffer(completed_wr_id as usize)?;
                            let (_cmd, lba, _virtual_addr, data_len, _r_key) = capsule_context
                                .get_request_capsule_content(completed_wr_id as usize)
                                .unwrap();
                            nvme_command_sx.send((completed_wr_id as u16, buffer_idx as usize * block_size, lba, true, data_len as usize)).unwrap();
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_WRITE => {
                            // This is a read I/O. This means the final remote write has been completed. Then, send response
                            capsule_context.set_response_status(completed_wr_id as usize, 0).unwrap();
                            let resp_sge = capsule_context
                                .get_resp_sge(completed_wr_id as usize)
                                .unwrap();
                            rdma_wr_sx.send(RDMAWorkRequest {
                                wr_id: completed_wr_id as u16,
                                sge: resp_sge,
                                mode: None,
                                remote_info: None
                            }).expect("PANIC: Failed to send IBV_WR_RDMA_SEND (response) command to the RDMA submission thread.");
                        }
                        _ => {}
                    }
                }

                any_inflight_wr = {
                    let rwm = rwm_mutex.lock().unwrap();
                    rwm.any_inflight_wr()
                };
            }

            Ok(0)
        }

        fn _run_rdma_submission_thread(
            client_context: Arc<ClientRdmaContext>,
            rwm_mutex: Arc<Mutex<RdmaWorkManager>>,
            rdma_wr_rx: Receiver<RDMAWorkRequest>
        ) -> Result<i32, RdmaTransportError> {
            loop {
                let rdma_wr = rdma_wr_rx.recv().unwrap();
                let qp = client_context.get_sendable_qp();

                if rdma_wr.mode.is_none() { // it means RDMA rcv request
                    {
                        let mut rwm = rwm_mutex.lock().unwrap();
                        rwm.post_rcv_req_work(rdma_wr.wr_id, qp, rdma_wr.sge).unwrap();
                    }
                    continue;
                }

                let mode = rdma_wr.mode.unwrap();
                match mode {
                    rdma_binding::ibv_wr_opcode_IBV_WR_SEND => {
                        post_send_response_work(rdma_wr.wr_id, qp, rdma_wr.sge).unwrap();
                    },
                    rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_READ => {
                        post_rmt_work(
                            rdma_wr.wr_id,
                            qp,
                            rdma_wr.sge,
                            rdma_wr.remote_info.unwrap().0,
                            rdma_wr.remote_info.unwrap().1,
                            rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_READ
                        ).expect("Panic: failed to post remote work");
                    },
                    rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE => {
                        post_rmt_work(
                            rdma_wr.wr_id,
                            qp,
                            rdma_wr.sge,
                            rdma_wr.remote_info.unwrap().0,
                            rdma_wr.remote_info.unwrap().1,
                            rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE
                        ).expect("Panic: failed to post remote work");
                    },
                    _ => {}
                }
            }

            Ok(0)
        }

        fn _run_nvme_device_thread(
            mut nvme_queue_pair: NvmeQueuePair,
            base_dma: ThreadSafeDmaHandle,
            running_signal: Arc<AtomicBool>,
            mut capsule_context: Arc<CapsuleContext>,
            buffer_l_key: u32,
            nvme_command_rx: Receiver<InternalNVMeCommandType>,
            rdma_wr_sx: Sender<RDMAWorkRequest>
        ) -> Result<i32, RdmaTransportError> {
            let mut c_id_to_offset_map: Vec<Option<usize>> = vec![None; QUEUE_LENGTH];
            let mut current_command = match nvme_command_rx.try_recv() {
                Ok(_cmd) => {
                    Some(_cmd)
                },
                Err(_) => None
            };

            while running_signal.load(Ordering::SeqCst) || current_command.is_some() {
                while let Some(completion) = nvme_queue_pair.quick_poll_completion() {
                    debug_println!("[NVMe Device] I/O is completed. cid = {}, status = {}", completion.c_id as u16, (completion.status >> 1) as u16);
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
                            debug_println_verbose!("Found NVMe device write I/O completion ....");
                            {
                                let mut capsule = capsule_context.get_resp_capsule(wr_id as usize).unwrap();
                                capsule.status = (completion.status >> 1) as i16;
                                capsule.cmd_id = wr_id;
                            }

                            rdma_wr_sx.send(RDMAWorkRequest {
                                wr_id,
                                sge: resp_sge,
                                mode: Some(rdma_binding::ibv_wr_opcode_IBV_WR_SEND),
                                remote_info: None,
                            }).expect("PANIC: failed to send RDMA WR via channel.");
                        },
                        2 => { // NVMe read is completed -> post RDMA remote write
                            let offset = c_id_to_offset_map[wr_id as usize].unwrap();
                            let mut local_sge = unsafe {
                                rdma_binding::ibv_sge {
                                    addr: base_dma.virt.add(offset) as u64,
                                    length: data_len,
                                    lkey: buffer_l_key,
                                }
                            };

                            rdma_wr_sx.send(RDMAWorkRequest {
                                wr_id,
                                sge: local_sge,
                                mode: Some(rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE),
                                remote_info: Some((virtual_addr, r_key)),
                            }).expect("PANIC: failed to send RDMA WR via channel.");
                            c_id_to_offset_map[wr_id as usize] = None;
                        },
                        _ => {}
                    }
                }

                if let Some((c_id, start_offset, lba, write, size)) = current_command {
                    c_id_to_offset_map[c_id as usize] = Some(start_offset);

                    unsafe {
                        debug_println!("[NVMe Device] Submit I/O {} command: bytes_offset: {}, lba: {}, size: {}", if write { "WRITE" } else { "READ" }, start_offset, lba, size);
                        nvme_queue_pair.submit_io_with_cid(
                            &base_dma.to_dma().slice(start_offset..start_offset + size),
                            lba,
                            write,
                            c_id
                        );
                    }
                }

                current_command = match nvme_command_rx.try_recv() {
                    Ok(_cmd) => {
                        Some(_cmd)
                    },
                    Err(_) => None
                };
            }

            Ok(0)
        }
    }
}

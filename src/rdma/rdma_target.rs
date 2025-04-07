pub mod rdma_target {
    use crate::rdma::buffer_manager::BufferManager;
    use crate::rdma::capsule::capsule::RequestCapsuleContext;
    use crate::rdma::rdma_common::rdma_binding::ibv_wc;
    use crate::rdma::rdma_common::rdma_common::{
        get_rdma_event_type_string, process_cm_event, ClientRdmaContext, RdmaTransportError, MAX_WR,
    };
    use crate::rdma::rdma_common::*;
    use crate::rdma::rdma_work_manager::rdma_work_manager::RdmaWorkManager;
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::net::Ipv4Addr;
    use std::ptr::null_mut;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::{io, mem, ptr, slice, thread};

    struct TargetRdmaContext {
        name: String,
        cm_event_channel: *mut rdma_binding::rdma_event_channel,
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
                cm_event_channel,
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
                rdma_binding::rdma_destroy_event_channel(self.cm_event_channel);
            }
        }
    }

    pub struct RdmaTarget {
        server_sockaddr: rdma_binding::sockaddr_in,
        ctx: TargetRdmaContext,
        client_handlers: Vec<JoinHandle<()>>,
        client_thread_signal: HashMap<String, Arc<AtomicBool>>,
        nvme_read_fn: fn(),  /*read function from the NVMe driver*/
        nvme_write_fn: fn(), /*write function from the NVMe driver*/
        buffer_manager_mtx_arc: Arc<Mutex<BufferManager>>,
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
            nvme_read_fn: fn(),
            nvme_write_fn: fn(),
            reserved_memory: usize,
            block_size: usize,
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
                nvme_read_fn: nvme_read_fn,
                nvme_write_fn: nvme_write_fn,
                buffer_manager_mtx_arc: Arc::new(Mutex::from(
                    BufferManager::new(reserved_memory, block_size).unwrap(),
                )),
                ctx: TargetRdmaContext::new(server_name.to_string(), cm_event_channel, cm_id),
            })
        }

        pub fn run(&mut self) -> Result<i32, RdmaTransportError> {
            let err_msg: String;
            let mut client_number = 0;

            // handle
            loop {
                let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
                let rc = process_cm_event(self.ctx.cm_event_channel, &mut cm_event)?;
                if rc != 0 {
                    err_msg = format!("{}: Failed to get cm event: {}", self.ctx.name, rc);
                    return Err(RdmaTransportError::OpFailedEx {
                        source: io::Error::last_os_error(),
                        message: err_msg,
                    });
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

                        let mr_ptr;

                        {
                            let mut buffer_manager = self.buffer_manager_mtx_arc.lock().unwrap();
                            mr_ptr = buffer_manager.register_mr(pd_ptr)?;
                            if mr_ptr.is_null() {
                                return Err(RdmaTransportError::FailedResourceInit("Buffer manager MR".parse().unwrap()))
                            }
                        }

                        let mut ctx_box = Box::new(ClientRdmaContext::new(cm_id_ptr, pd_ptr, mr_ptr, MAX_WR as u16)?);

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

                        unsafe {
                            debug_println_verbose!("accept connection");
                            let rc = rdma_binding::rdma_accept(ctx_box.cm_id, &mut conn_param);

                            if rc != 0 {
                                let err_msg = format!("{}: Failed to accept the connection. {}", ctx_box._name, rc);
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
                            let rc = process_cm_event(self.ctx.cm_event_channel, &mut cm_event)?;
                            if rc != 0 {
                                let err_msg = format!("{}: Failed to get cm event: {}", ctx_box._name, rc);
                                return Err(RdmaTransportError::OpFailedEx {
                                    source: io::Error::last_os_error(),
                                    message: err_msg
                                });
                            }

                            let e_type = (*cm_event).event;
                            debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                            if e_type != rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED {
                                let err_msg = format!("{}: Received even with type RDMA_CM_EVENT_ESTABLISHED. Got {} instead.", ctx_box._name, e_type);
                                return Err(RdmaTransportError::OpFailedEx {
                                    source: io::Error::last_os_error(),
                                    message: err_msg
                                });
                            }

                            rdma_binding::rdma_ack_cm_event(cm_event);

                            if rc != 0 {
                                let err_msg = format!("{}: Failed to acknowledge the cm event: {}", ctx_box._name, rc);
                                return Err(RdmaTransportError::OpFailedEx {
                                    source: io::Error::last_os_error(),
                                    message: err_msg
                                });
                            }
                        }

                        debug_println!("A new connection is accepted.");
                        let signal = Arc::new(AtomicBool::new(true));
                        self.client_thread_signal.insert(ctx_box._name.clone(), Arc::clone(&signal));
                        let thread_signal = signal.clone();
                        let thread_buffer_manager = self.buffer_manager_mtx_arc.clone();

                        let handle = thread::spawn({
                            let nvme_read_fn = self.nvme_read_fn;
                            let nvme_write_fn = self.nvme_write_fn;
                            move || {
                                RdmaTarget::_handle_client(ctx_box, thread_signal, thread_buffer_manager, nvme_read_fn, nvme_write_fn)
                                    .expect(format!("PANIC: handling client thread {}", client_number.to_string()).as_str());
                            }
                        });

                        client_number = client_number + 1;
                        self.client_handlers.push(handle);
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

            for handle in self.client_handlers {
                handle.join().unwrap();
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

        /// Upon receiving request, first it reads the op_code. Then the logic branches as follows:
        ///     - I/O write:
        ///         1. Allocate local buffer to receive data based on the length information
        ///         2. Post RDMA read operation using the same wr_id
        fn _on_request_received(
            wr_id: u16,
            ctx: &mut ClientRdmaContext,
            rwm: &mut RdmaWorkManager,
            buffer_manager_arc: Arc<Mutex<BufferManager>>,
        ) -> Result<(), RdmaTransportError> {
            let qp = unsafe { (*ctx.cm_id).qp };

            let data_mr_len = {
                let (capsule, _) = ctx
                    .req_capsule_ctx
                    .as_mut()
                    .get_capsule_pair(wr_id as usize)
                    .unwrap();

                capsule.data_mr_length as usize
            };
            let cmd_opcode = {
                let (capsule, _) = ctx
                    .req_capsule_ctx
                    .as_mut()
                    .get_capsule_pair(wr_id as usize)
                    .unwrap();

                capsule.cmd.opcode.clone()
            };
            debug_println!(
                "received I/O request. cmd_opcode: {}, len: {}",
                cmd_opcode,
                data_mr_len
            );

            match cmd_opcode {
                1 => {
                    // I/O write command
                    let mut local_buffer;
                    {
                        let mut buffer_manager = buffer_manager_arc.lock().unwrap();
                        local_buffer = buffer_manager.allocate().unwrap(); // TODO(what should we do when there is no available buffer?)
                    }
                    ctx.set_memory_block(wr_id as usize, &mut local_buffer);
                    let data_addr;
                    let data_len;
                    let data_rkey;
                    let req_capsule;
                    {
                        let req_capsule_ctx = ctx.req_capsule_ctx.as_mut();
                        req_capsule = req_capsule_ctx
                            .req_capsules
                            .as_slice()
                            .as_ref()
                            .get(wr_id as usize)
                            .unwrap();
                        data_addr = req_capsule.data_mr_address.clone();
                        data_rkey = req_capsule.data_mr_r_key.clone();
                        data_len = req_capsule.data_mr_length.clone();
                    }

                    debug_println!(
                        "[POSTING REMOTE READ]. remote_addr: {}, len: {}, rkey: {}",
                        data_addr,
                        data_len,
                        data_rkey
                    );
                    let lkey = ctx.get_local_buffer_lkey();
                    rwm.post_rmt_work(
                        wr_id,
                        qp,
                        local_buffer.get(),
                        lkey,
                        data_addr,
                        data_rkey,
                        data_len,
                        rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_READ,
                    )
                    .unwrap();
                }
                2 => {
                    // I/O read command
                    // TODO(send_nvme_io_read()) should be asynchronous. Hence, rwm.post_rmt_work should not be here.
                    let mut local_buffer;
                    {
                        let mut buffer_manager = buffer_manager_arc.lock().unwrap();
                        local_buffer = buffer_manager.allocate().unwrap(); // TODO(what should we do when there is no available buffer?)
                    }
                    ctx.set_memory_block(wr_id as usize, &mut local_buffer);

                    unsafe {
                        let local_buffer = local_buffer.get();
                        let pattern = b"dcba";
                        let slice = std::slice::from_raw_parts_mut(local_buffer, 512);
                        for i in 0..512 {
                            slice[i] = pattern[i % pattern.len()];
                        }
                    }

                    let lkey = ctx.get_local_buffer_lkey().clone();
                    let data_addr;
                    let data_len;
                    let data_rkey;

                    {
                        let req_capsule_ctx = ctx.req_capsule_ctx.as_mut();
                        let req_capsule = req_capsule_ctx
                            .req_capsules
                            .as_slice()
                            .as_ref()
                            .get(wr_id as usize)
                            .unwrap();
                        data_addr = req_capsule.data_mr_address.clone();
                        data_rkey = req_capsule.data_mr_r_key.clone();
                        data_len = req_capsule.data_mr_length.clone();
                    }

                    rwm.post_rmt_work(
                        wr_id,
                        qp,
                        local_buffer.get(),
                        lkey,
                        data_addr,
                        data_rkey,
                        data_len,
                        rdma_binding::ibv_wr_opcode_IBV_WR_RDMA_WRITE,
                    )
                    .unwrap();
                }
                _ => {}
            }
            Ok(())
        }

        // Handle connection request
        // Steps:
        //     - ACK the RDMA_CM_EVENT_CONNECT_REQUEST event
        //     - Setup client resources (at this moment, we only support a single client)
        //     - Accept the connection via rdma_binding::rdma_accept(2)
        //     - Exchange metadata with client
        fn _handle_client(
            mut client_context_box: Box<ClientRdmaContext>,
            running_signal: Arc<AtomicBool>,
            buffer_manager_arc: Arc<Mutex<BufferManager>>,
            read_fn: fn(),
            write_fn: fn(),
        ) -> Result<i32, RdmaTransportError> {
            let cl_ctx = client_context_box.as_mut();
            let mut rdma_work_manager = RdmaWorkManager::new(MAX_WR as u16);
            let qp = unsafe { (*cl_ctx.cm_id).qp };
            let cq = cl_ctx.cq;
            let capsule_ctx = cl_ctx.req_capsule_ctx.as_mut() as *mut RequestCapsuleContext;
            debug_println_verbose!("Handling client thread start.");

            // Initially post recv WR
            debug_println_verbose!("Posting rcv work");
            while let Some(wr_id) = rdma_work_manager.allocate_wr_id() {
                rdma_work_manager
                    .post_rcv_req_work(wr_id, qp, cq, capsule_ctx)
                    .unwrap();
            }
            debug_println_verbose!("Posting rcv work: success");

            while running_signal.load(Ordering::SeqCst) || rdma_work_manager.any_inflight_wr() {
                // idea: for every loop:
                //  - poll_completion
                //  - loop over the completed WCs
                //  - post next works accordingly
                debug_println!("Polling completion....");
                rdma_work_manager
                    .poll_completed_works(cl_ctx.io_comp_channel, cl_ctx.cq)
                    .unwrap();

                while true {
                    let completed_wr_id;
                    let op_code;
                    let op_code_str;
                    let wc_status;

                    {
                        let wc_opt = rdma_work_manager.next_wc();
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

                    let status_str = unsafe {
                        CStr::from_ptr(rdma_binding::ibv_wc_status_str(wc_status))
                            .to_string_lossy() // converts to Cow<str>, handles invalid UTF-8 safely
                            .into_owned()
                    };

                    debug_println_verbose!(
                        "Got a completion wr_id: {}, op_code: {}, status: {}",
                        completed_wr_id,
                        op_code_str,
                        status_str
                    );

                    if !running_signal.load(Ordering::SeqCst) {
                        {
                            rdma_work_manager
                                .release_wr(completed_wr_id as u16)
                                .map_err(|_| {
                                    RdmaTransportError::OpFailed("failed to release WR".into())
                                })?;
                        }
                        continue;
                    }
                    // TODO(handle accordingly)
                    if wc_status != rdma_binding::ibv_wc_status_IBV_WC_SUCCESS {
                        {
                            rdma_work_manager
                                .release_wr(completed_wr_id as u16)
                                .map_err(|_| {
                                    RdmaTransportError::OpFailed("failed to release WR".into())
                                })?;
                        }

                        // TODO(what should we do?)
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
                            // create function _process_io_request()
                            // submit new recv work
                            let buffer_manager_clone = buffer_manager_arc.clone();
                            RdmaTarget::_on_request_received(
                                completed_wr_id as u16,
                                cl_ctx,
                                &mut rdma_work_manager,
                                buffer_manager_clone,
                            )?;
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_SEND => {
                            // means response capsule is sent. Practically do nothing then release the wr_id
                            rdma_work_manager
                                .release_wr(completed_wr_id as u16)
                                .unwrap();
                            let new_wr_id = rdma_work_manager.allocate_wr_id().unwrap();
                            rdma_work_manager
                                .post_rcv_req_work(
                                    new_wr_id,
                                    qp,
                                    cq,
                                    cl_ctx.req_capsule_ctx.as_mut(),
                                )
                                .unwrap();
                            debug_println!("Response is sent for wr_id: {}", completed_wr_id);
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_READ => {
                            // This is a write I/O. Hence, call the send_nvme_io_write()

                            // Dummy implementation: print the RDMA read result.
                            let local_buffer_wrapped =
                                cl_ctx.free_remote_op_buffer(completed_wr_id as usize)?;
                            let local_buffer = local_buffer_wrapped.get();
                            assert!(!local_buffer.is_null());
                            let req_capsule_ctx = cl_ctx.req_capsule_ctx.as_mut();
                            let req_capsule = req_capsule_ctx
                                .req_capsules
                                .as_slice()
                                .as_ref()
                                .get(completed_wr_id as usize)
                                .unwrap();
                            let size = req_capsule.data_mr_length;
                            let slice =
                                unsafe { slice::from_raw_parts(local_buffer, size as usize) }; // Create a slice
                            match std::str::from_utf8(slice) {
                                Ok(s) => debug_println!("Buffer as string: {}", s),
                                Err(e) => debug_println!("Invalid UTF-8: {}", e),
                            }

                            // TODO(send_nvme_io_write()) should be asynchronous. Hence, rwm.post_rmt_work should not be here.
                            let (req_capsule, resp_capsule) = cl_ctx
                                .req_capsule_ctx
                                .get_capsule_pair(completed_wr_id as usize)
                                .unwrap();
                            let nvme_cid = req_capsule.cmd.c_id;
                            resp_capsule.cmd_id = nvme_cid.clone();
                            resp_capsule.status = 0;

                            {
                                let mut buffer_manager = buffer_manager_arc.lock().unwrap();
                                buffer_manager.free(local_buffer_wrapped);
                            }

                            debug_println_verbose!(
                                "posting response for wr_id: {} cm_id: {}",
                                completed_wr_id,
                                nvme_cid
                            );
                            rdma_work_manager
                                .post_send_response_work(
                                    completed_wr_id as u16,
                                    qp,
                                    cq,
                                    cl_ctx.req_capsule_ctx.as_mut(),
                                )
                                .unwrap();
                            debug_println!(
                                "[SUCCESS] posting response for wr_id: {} cm_id: {}",
                                completed_wr_id,
                                nvme_cid
                            );
                        }
                        rdma_binding::ibv_wc_opcode_IBV_WC_RDMA_WRITE => {
                            // This is a read I/O. Hence, directly send response capsule
                            {
                                let (req_capsule, resp_capsule) = cl_ctx
                                    .req_capsule_ctx
                                    .get_capsule_pair(completed_wr_id as usize)
                                    .unwrap();
                                let nvme_cid = req_capsule.cmd.c_id;
                                resp_capsule.cmd_id = nvme_cid.clone();
                                resp_capsule.status = 0;
                            }

                            let local_buffer_wrapper =
                                cl_ctx.free_remote_op_buffer(completed_wr_id as usize)?;

                            {
                                let mut buffer_manager = buffer_manager_arc.lock().unwrap();
                                buffer_manager.free(local_buffer_wrapper);
                            }

                            rdma_work_manager
                                .post_send_response_work(
                                    completed_wr_id as u16,
                                    qp,
                                    cq,
                                    cl_ctx.req_capsule_ctx.as_mut(),
                                )
                                .unwrap();
                        }
                        _ => {}
                    }
                }
            }

            // TODO(clean up everything)

            Ok(0)
        }
    }
}

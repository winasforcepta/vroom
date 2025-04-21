pub mod rdma_initiator {
    use crate::rdma::buffer_manager::{BufferManager, RdmaBufferBlock};
    use crate::rdma::capsule::capsule::RequestCapsuleContext;
    use crate::rdma::rdma_common::rdma_binding;
    use crate::rdma::rdma_common::rdma_common::{
        get_rdma_event_type_string, process_cm_event, ClientRdmaContext, RdmaTransportError, MAX_WR,
    };
    use crate::rdma::rdma_work_manager::rdma_work_manager::RdmaWorkManager;
    use crate::debug_println_verbose;
    use std::net::Ipv4Addr;
    use std::{mem, ptr};

    pub struct RdmaInitiator {
        server_sockaddr: rdma_binding::sockaddr_in,
        ctx: ClientRdmaContext,
        pub(crate) rwm: RdmaWorkManager,
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

    impl RdmaInitiator {
        pub fn connect(
            ipv4addr: Ipv4Addr,
            port: u16,
            buffer_manager: &mut BufferManager,
        ) -> Result<Self, RdmaTransportError> {
            let err_msg: String;
            let mut server_sockaddr = rdma_binding::sockaddr_in {
                sin_family: rdma_binding::AF_INET as rdma_binding::sa_family_t,
                sin_port: port.to_be(),
                sin_addr: rdma_binding::in_addr {
                    s_addr: u32::from(ipv4addr).to_be(),
                },
                sin_zero: unsafe { mem::zeroed() },
            };
            let mut event_channel_box;

            // Prepare the connection
            // Open a channel used to report asynchronous communication event
            debug_println_verbose!("initiator setup: creating event channel...");
            unsafe {
                let event_channel = rdma_binding::rdma_create_event_channel();
                if event_channel.is_null() {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to create event channel".parse().unwrap(),
                    ));
                }
                event_channel_box = Box::from_raw(event_channel);
            }
            debug_println_verbose!("initiator setup: event channel is created.");

            // Create cm_id
            // rdma_cm_id is the connection identifier (like socket) which is used
            // to define an RDMA connection.
            debug_println_verbose!("initiator setup: creating event channel...");
            let mut cm_id_ptr = ptr::null_mut();
            unsafe {
                let rc = rdma_binding::rdma_create_id(
                    event_channel_box.as_mut(),
                    &mut cm_id_ptr,
                    ptr::null_mut(),
                    rdma_binding::rdma_port_space_RDMA_PS_TCP,
                );
                if rc != 0 || cm_id_ptr.is_null() {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to create RDMA cm_id".parse().unwrap(),
                    ));
                }
            }
            debug_println_verbose!("Initiator setup: CM ID is created.");

            // Resolve destination and optional source addresses from IP addresses  to
            // an RDMA address.  If successful, the specified rdma_cm_id will be bound
            // to a local device.
            let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
            let s_addr_ptr: *mut rdma_binding::sockaddr_in = &mut server_sockaddr;
            let s_ptr: *mut rdma_binding::sockaddr = s_addr_ptr as *mut rdma_binding::sockaddr;
            unsafe {
                debug_println_verbose!("resolving address");
                let rc = rdma_binding::rdma_resolve_addr(cm_id_ptr, ptr::null_mut(), s_ptr, 2000);

                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to resolve address".parse().unwrap(),
                    ));
                }

                debug_println_verbose!("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED");
                let rc = process_cm_event(event_channel_box.as_mut(), &mut cm_event)?;
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to resolve address: receiving cm_event"
                            .parse()
                            .unwrap(),
                    ));
                }

                let e_type = (*cm_event).event;
                debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                if e_type != rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_RESOLVED {
                    err_msg = format!("Expecting RDMA_CM_EVENT_ADDR_RESOLVED, got {} instead: Failed to resolve address", get_rdma_event_type_string(e_type));
                    return Err(RdmaTransportError::OpFailed(err_msg));
                }
                let rc = rdma_binding::rdma_ack_cm_event(cm_event);
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to resolve address: ack event".parse().unwrap(),
                    ));
                }
            }

            // Resolves an RDMA route to the destination address in order to
            //  establish a connection
            let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
            debug_println_verbose!("Initiator setup: rdma_resolve_route.");
            unsafe {
                let rc = rdma_binding::rdma_resolve_route(cm_id_ptr, 2000);
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to resolve route".parse().unwrap(),
                    ));
                }
                debug_println_verbose!("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED");
                let rc = process_cm_event(event_channel_box.as_mut(), &mut cm_event)?;
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to resolve route: receiving cm_event"
                            .parse()
                            .unwrap(),
                    ));
                }
                let e_type = (*cm_event).event;
                debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                if e_type != rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_RESOLVED {
                    err_msg = format!("Expecting RDMA_CM_EVENT_ROUTE_RESOLVED, got {} instead: Failed to resolve address", get_rdma_event_type_string(e_type));
                    return Err(RdmaTransportError::OpFailedEx {
                        source: std::io::Error::last_os_error(),
                        message: err_msg,
                    });
                }
                let rc = rdma_binding::rdma_ack_cm_event(cm_event);
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to resolve route: ack event".parse().unwrap(),
                    ));
                }
            }

            // setup client resources
            debug_println_verbose!("resource setup: Setting up context");
            let mut pd_ptr;
            unsafe {
                pd_ptr = rdma_binding::ibv_alloc_pd((*cm_id_ptr).verbs);
                if pd_ptr.is_null() {
                    return Err(RdmaTransportError::FailedResourceInit(
                        "protection domain".parse().unwrap(),
                    ));
                }
            }
            let mr_ptr;

            {
                mr_ptr = buffer_manager.register_mr(pd_ptr)?;
                if mr_ptr.is_null() {
                    return Err(RdmaTransportError::FailedResourceInit(
                        "Buffer manager MR".parse().unwrap(),
                    ));
                }
            }
            let mut ctx = ClientRdmaContext::new(cm_id_ptr, pd_ptr, mr_ptr, MAX_WR as u16)?;
            debug_println_verbose!("resource setup: context created.");

            debug_println_verbose!("Trying to connect to the server");
            let mut cm_event: *mut rdma_binding::rdma_cm_event = ptr::null_mut();
            unsafe {
                let mut conn_param: rdma_binding::rdma_conn_param = mem::zeroed();
                conn_param.initiator_depth = 3;
                conn_param.responder_resources = 3;
                conn_param.retry_count = 3; // if fails, then how many times to retry
                let rc = rdma_binding::rdma_connect(ctx.cm_id, &mut conn_param);
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to connect to server.".parse().unwrap(),
                    ));
                }
                debug_println_verbose!("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED");
                debug_println_verbose!("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED");
                let rc = process_cm_event(event_channel_box.as_mut(), &mut cm_event)?;
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to connect: receiving cm_event".parse().unwrap(),
                    ));
                }
                let e_type = (*cm_event).event;
                debug_println_verbose!("Got an event {}", get_rdma_event_type_string(e_type));

                if e_type != rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED {
                    err_msg = format!("Expecting rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED, got {} instead: Failed to resolve address", get_rdma_event_type_string(e_type));
                    return Err(RdmaTransportError::OpFailedEx {
                        source: std::io::Error::last_os_error(),
                        message: err_msg,
                    });
                }
                let rc = rdma_binding::rdma_ack_cm_event(cm_event);
                if rc != 0 {
                    return Err(RdmaTransportError::OpFailed(
                        "Failed to connect: ack event".parse().unwrap(),
                    ));
                }
            }

            debug_println!("The client is connected successfully");
            Ok(Self {
                server_sockaddr,
                ctx,
                rwm: RdmaWorkManager::new(MAX_WR as u16),
            })
        }

        pub fn post_remote_io_write(
            &mut self,
            nvme_cid: u16,
            nvme_addr: u64,
            local_buffer: &mut RdmaBufferBlock,
            data_len: u32,
        ) -> Result<i32, RdmaTransportError> {
            let wr_id;
            match self.rwm.allocate_wr_id() {
                None => return Err(RdmaTransportError::OpFailed("Can't allocate WR ID".into())),
                Some(id) => {
                    wr_id = id;
                }
            }

            let rkey = self.ctx.get_local_buffer_rkey();

            {
                let capsule = &mut self.ctx.req_capsule_ctx.req_capsules[wr_id as usize];
                capsule.lba = nvme_addr;
                capsule.cmd.c_id = nvme_cid;
                capsule.cmd.opcode = 1;
                capsule.data_mr_address = local_buffer.as_ptr() as u64;
                capsule.data_mr_r_key = rkey;
                capsule.data_mr_length = data_len;
            }
            debug_println!(
                "[capsule data] nvme_add={} data_addr={} data_rkey={}, len={}",
                nvme_addr,
                local_buffer.get() as u64,
                rkey,
                data_len
            );

            // assign the buffer containing the data
            self.ctx
                .set_memory_block(wr_id as usize, local_buffer);
            let qp = unsafe { (*self.ctx.cm_id).qp };
            let cq = self.ctx.cq;
            let req_capsule_ctx = self.ctx.req_capsule_ctx.as_mut() as *mut RequestCapsuleContext;
            // First post the rcv work to prepare for response
            self.rwm
                .post_send_request_work(wr_id, qp, req_capsule_ctx)
                .map_err(|_| {
                    RdmaTransportError::OpFailed("failed to post send request WR".into())
                })?;
            self.rwm
                .post_rcv_resp_work(wr_id, qp, cq, req_capsule_ctx)
                .unwrap();

            Ok(0)
        }

        pub fn post_remote_io_read(
            &mut self,
            nvme_cid: u16,
            nvme_addr: u64,
            local_buffer: &mut RdmaBufferBlock,
            data_len: u32,
        ) -> Result<i32, RdmaTransportError> {
            let wr_id = self.rwm.allocate_wr_id().unwrap();
            let rkey = self.ctx.get_local_buffer_rkey();

            {
                let capsule = &mut self.ctx.req_capsule_ctx.req_capsules[wr_id as usize];
                capsule.lba = nvme_addr;
                capsule.cmd.c_id = nvme_cid;
                capsule.cmd.opcode = 1;
                capsule.data_mr_address = local_buffer.as_ptr() as u64;
                capsule.data_mr_r_key = rkey;
                capsule.data_mr_length = data_len;
            }

            debug_println!(
                "[capsule data] nvme_add={} data_addr={} data_rkey={}, len={}",
                nvme_addr,
                local_buffer.get() as u64,
                rkey,
                data_len
            );

            // assign the buffer containing the data
            self.ctx
                .set_memory_block(wr_id as usize, local_buffer);
            let qp = unsafe { (*self.ctx.cm_id).qp };
            let cq = self.ctx.cq;
            let req_capsule_ctx = self.ctx.req_capsule_ctx.as_mut() as *mut RequestCapsuleContext;
            // First post the rcv work to prepare for response
            self.rwm
                .post_rcv_resp_work(wr_id, qp, cq, req_capsule_ctx)
                .unwrap();
            self.rwm
                .post_send_request_work(wr_id, qp, req_capsule_ctx)
                .map_err(|_| {
                    RdmaTransportError::OpFailed("failed to post send request WR".into())
                })?;

            Ok(0)
        }

        pub fn poll_completions(&mut self) -> Result<(u16, u16), RdmaTransportError> {
            let mut n_successes = 0;
            let mut n_failed = 0;
            self.rwm
                .poll_completed_works(self.ctx.io_comp_channel, self.ctx.cq)
                .unwrap();
            let mut ret = 0;

            while let Some(wc) = self.rwm.next_wc() {
                let wr_id = wc.wr_id.clone();
                let op_code = wc.opcode.clone();

                if op_code == rdma_binding::ibv_wc_opcode_IBV_WC_RECV {
                    if wc.status != rdma_binding::ibv_wc_status_IBV_WC_SUCCESS {
                        debug_println!(
                            "[UNSUCCESSFUL] wc {} is not success: opcode={}, status={}",
                            wc.wr_id,
                            wc.opcode,
                            wc.status
                        );
                        n_failed += 1;
                    } else {
                        n_successes += 1;
                    }
                    self.rwm.release_wr(wr_id as u16).unwrap();
                }
            }

            Ok((n_successes, n_failed))
        }
    }
}

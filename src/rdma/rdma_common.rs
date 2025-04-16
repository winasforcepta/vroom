#[allow(unused)]
pub mod rdma_binding {
    #[allow(
        non_camel_case_types,
        non_snake_case,
        non_upper_case_globals,
        unused,
        dead_code
    )]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub mod rdma_common {
    use crate::rdma::buffer_manager::{RdmaBufferBlock};
    use crate::rdma::capsule::capsule::RequestCapsuleContext;
    use crate::rdma::rdma_common::rdma_binding;
    use std::any::Any;
    use std::net::Ipv4Addr;
    use std::os::raw::c_int;
    use std::{fmt, io, mem, ptr};

    pub static CQ_CAPACITY: u32 = 16384u32;
    pub static MAX_SGE: u32 = 1u32;
    pub static MAX_WR: u32 = 16384u32;

    #[derive(Debug)]
    pub enum RdmaTransportError {
        Custom(String),
        OpFailedEx { message: String, source: io::Error },
        OpFailed(String),
        FailedResourceInit(String),
        PanicOccurred(String),
    }
    impl fmt::Display for RdmaTransportError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                RdmaTransportError::Custom(msg) => write!(f, "Custom error occurred: {}", msg),
                RdmaTransportError::OpFailedEx { message, source } => {
                    write!(f, "RDMA Operation failed: {}. Source: {}", message, source)
                }
                RdmaTransportError::FailedResourceInit(resource) => {
                    let msg = format!("{} initialization failed", resource);
                    write!(f, "{}", msg)
                }
                RdmaTransportError::OpFailed(message) => write!(f, "RDMA op failed: {}", message),
                RdmaTransportError::PanicOccurred(message) => write!(f, "{}", message),
            }
        }
    }

    impl From<Box<dyn Any + Send>> for RdmaTransportError {
        fn from(_: Box<dyn Any + Send>) -> Self {
            RdmaTransportError::PanicOccurred("panic occurred".into())
        }
    }

    pub fn process_cm_event(
        event_channel: *mut rdma_binding::rdma_event_channel,
        cm_event: *mut *mut rdma_binding::rdma_cm_event,
    ) -> Result<i32, RdmaTransportError> {
        let rc: i32;
        assert!(
            !event_channel.is_null(),
            "[_process_event] event_channel should not be null."
        );
        unsafe {
            rc = rdma_binding::rdma_get_cm_event(event_channel as *mut _, cm_event);
        }

        if rc != 0 {
            let err_msg = format!("Failed to retrieve a cm event: {}", rc);
            return Err(RdmaTransportError::OpFailedEx {
                source: std::io::Error::last_os_error(),
                message: err_msg,
            });
        }

        /* let's see, if it was a good event */
        let e_status = unsafe { (**cm_event).status };
        if e_status != 0 {
            if e_status == -110 {
                // -110 means connection timeout. ACK-ing this event would result panic
            } else {
                unsafe { rdma_binding::rdma_ack_cm_event(*cm_event) };
            }

            return Ok(e_status);
        }

        Ok(rc)
    }

    pub fn get_rdma_event_type_string(e_type: rdma_binding::rdma_cm_event_type) -> String {
        match e_type {
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_RESOLVED => {
                "RDMA_CM_EVENT_ADDR_RESOLVED".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_ERROR => {
                "RDMA_CM_EVENT_ADDR_ERROR".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_RESOLVED => {
                "RDMA_CM_EVENT_ROUTE_RESOLVED".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_ERROR => {
                "RDMA_CM_EVENT_ROUTE_ERROR".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_REQUEST => {
                "RDMA_CM_EVENT_CONNECT_REQUEST".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_RESPONSE => {
                "RDMA_CM_EVENT_CONNECT_RESPONSE".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_ERROR => {
                "RDMA_CM_EVENT_CONNECT_ERROR".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_UNREACHABLE => {
                "RDMA_CM_EVENT_UNREACHABLE".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_REJECTED => {
                "RDMA_CM_EVENT_REJECTED".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED => {
                "RDMA_CM_EVENT_ESTABLISHED".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED => {
                "RDMA_CM_EVENT_DISCONNECTED".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_DEVICE_REMOVAL => {
                "RDMA_CM_EVENT_DEVICE_REMOVAL".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_JOIN => {
                "RDMA_CM_EVENT_MULTICAST_JOIN".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_ERROR => {
                "RDMA_CM_EVENT_MULTICAST_ERROR".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_CHANGE => {
                "RDMA_CM_EVENT_ADDR_CHANGE".to_string()
            }
            rdma_binding::rdma_cm_event_type_RDMA_CM_EVENT_TIMEWAIT_EXIT => {
                "RDMA_CM_EVENT_TIMEWAIT_EXIT".to_string()
            }
            16_u32..=u32::MAX => "UNKNOWN".to_string(),
        }
    }

    pub(crate) struct ClientRdmaContext {
        pub(crate) _name: String,
        pub(crate) cm_id: *mut rdma_binding::rdma_cm_id,
        pub(crate) pd: *mut rdma_binding::ibv_pd,
        pub(crate) io_comp_channel: *mut rdma_binding::ibv_comp_channel,
        pub(crate) cq: *mut rdma_binding::ibv_cq,
        pub(crate) req_capsule_ctx: Box<RequestCapsuleContext>,
        remote_buffers: Vec<Option<RdmaBufferBlock>>,
        buffer_mr: *mut rdma_binding::ibv_mr,
    }

    impl ClientRdmaContext {
        pub fn new(
            mut cm_id_ptr: *mut rdma_binding::rdma_cm_id,
            mut pd_ptr: *mut rdma_binding::ibv_pd,
            buffer_manager_mr_ptr: *mut rdma_binding::ibv_mr,
            max_wr: u16,
        ) -> Result<Self, RdmaTransportError> {
            assert!(!cm_id_ptr.is_null(), "cm_id_ptr is null");
            let verb = unsafe { (*cm_id_ptr).verbs };
            let mut io_comp_channel;

            unsafe {
                io_comp_channel = rdma_binding::ibv_create_comp_channel(verb);
                if io_comp_channel.is_null() {
                    return Err(RdmaTransportError::FailedResourceInit(
                        "IO Completion Channel".into(),
                    ));
                }
            }

            let cq;
            unsafe {
                cq = rdma_binding::ibv_create_cq(
                    verb,                 /* which device */
                    CQ_CAPACITY as c_int, /* maximum capacity*/
                    ptr::null_mut(),      /* user context, not used here */
                    io_comp_channel,      /* which IO completion channel */
                    0,                    /* signaling vector, not used here*/
                );

                if cq.is_null() {
                    return Err(RdmaTransportError::FailedResourceInit(
                        "Completion Queue".parse().unwrap(),
                    ));
                }
            }

            let mut qp_init_attr = rdma_binding::ibv_qp_init_attr {
                qp_context: ptr::null_mut(), // optional
                send_cq: cq,
                recv_cq: cq,
                srq: ptr::null_mut(), // optional,
                cap: rdma_binding::ibv_qp_cap {
                    max_send_wr: MAX_WR,
                    max_recv_wr: MAX_WR,
                    max_send_sge: MAX_SGE,
                    max_recv_sge: MAX_SGE,
                    max_inline_data: 1,
                },
                qp_type: rdma_binding::ibv_qp_type_IBV_QPT_RC,
                sq_sig_all: 0,
            };

            unsafe {
                let rc = rdma_binding::rdma_create_qp(cm_id_ptr, pd_ptr, &mut qp_init_attr);
                if rc != 0 {
                    return Err(RdmaTransportError::FailedResourceInit(
                        format!("Queue Pair. {}", rc).parse().unwrap(),
                    ));
                }
            }

            let req_capsule_ctx = RequestCapsuleContext::new(pd_ptr, max_wr).map_err(|_| {
                RdmaTransportError::OpFailed("failed to construct IO request context".into())
            })?;

            Ok(Self {
                _name: format!("Client {}", Self::_get_client_address(cm_id_ptr)),
                cm_id: cm_id_ptr,
                pd: pd_ptr,
                io_comp_channel,
                cq,
                req_capsule_ctx,
                remote_buffers: std::iter::repeat_with(|| None)
                    .take(max_wr as usize)
                    .collect(),
                buffer_mr: buffer_manager_mr_ptr,
            })
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

        pub fn set_memory_block(&mut self, idx: usize, buffer_ptr: *mut RdmaBufferBlock) {
            assert!(!buffer_ptr.is_null());
            assert!(unsafe { !(*buffer_ptr).as_ptr().is_null() });
            self.remote_buffers[idx] = Some(unsafe { *buffer_ptr });
        }

        pub fn get_remote_op_buffer(
            &self,
            idx: usize,
        ) -> Result<*mut RdmaBufferBlock, RdmaTransportError> {
            assert!(self.remote_buffers[idx].is_some());
            Ok(&mut self.remote_buffers[idx].unwrap())
        }

        pub fn free_remote_op_buffer(
            &mut self,
            idx: usize,
        ) -> Result<RdmaBufferBlock, RdmaTransportError> {
            assert!(self.remote_buffers[idx].is_some());
            let ret = self.remote_buffers[idx].unwrap();
            self.remote_buffers[idx] = None;
            Ok(ret)
        }

        pub fn get_local_buffer_lkey(&self) -> u32 {
            assert!(!self.buffer_mr.is_null());
            unsafe { (*self.buffer_mr).lkey.clone() }
        }
        pub fn get_local_buffer_rkey(&self) -> u32 {
            assert!(!self.buffer_mr.is_null());
            unsafe { (*self.buffer_mr).rkey.clone() }
        }
    }

    impl Drop for ClientRdmaContext {
        fn drop(&mut self) {
            for i in 0..self.remote_buffers.len() {
                if !self.remote_buffers[i].is_none() {
                    self.remote_buffers[i] = None;
                }
            }

            unsafe {
                println!("Transitioning QP into IBV_QPS_ERR.");
                let mut qp_attr: rdma_binding::ibv_qp_attr = mem::zeroed();
                qp_attr.qp_state = rdma_binding::ibv_qp_state_IBV_QPS_ERR;
                let rc = rdma_binding::ibv_modify_qp((*self.cm_id).qp, &mut qp_attr, 0x1);
                if rc != 0 {
                    eprintln!("{}: Failed to set QP to ERR state", self._name);
                }
                println!("QP is transitioned into IBV_QPS_ERR.");
            }

            unsafe {
                println!("Flushing WC.");
                let mut dummy: Vec<rdma_binding::ibv_wc> = vec![mem::zeroed(); MAX_WR as usize];
                rdma_binding::ibv_poll_cq_ex(self.cq, MAX_WR as c_int, dummy.as_mut_ptr());
                println!("WC is flushed");
            }

            // unsafe {
            //     println!("Drain & ack CQ events");
            //     let mut event_cq: *mut rdma_binding::ibv_cq = std::ptr::null_mut();
            //     let mut context: *mut c_void = std::ptr::null_mut();
            //
            //     // Try to get one event (non-blocking version optional)
            //     while rdma_binding::ibv_get_cq_event(self.io_comp_channel, &mut event_cq, &mut context) == 0 {
            //         if event_cq == self.cq {
            //             println!("CQ event received, acking.");
            //             rdma_binding::ibv_ack_cq_events(self.cq, 1);
            //         } else {
            //             eprintln!("Warning: Event received for unknown CQ.");
            //         }
            //     }
            // }

            unsafe {
                println!("Dropping QP.");
                rdma_binding::rdma_destroy_qp(self.cm_id);
                println!("QP is successfully manually dropped.");
            }

            // unsafe {
            //     println!("Dropping CQ.");
            //     let rc = rdma_binding::ibv_destroy_cq(self.cq);
            //     if rc != 0 {
            //         eprintln!("{}: ibv_destroy_cq() failed.", self._name)
            //     } else {
            //         println!("cq is successfully manually dropped.");
            //     }
            // }

            unsafe {
                println!("Dropping io_comp_channel.");
                let rc = rdma_binding::ibv_destroy_comp_channel(self.io_comp_channel);
                if rc != 0 {
                    eprintln!("{}: ibv_destroy_comp_channel() failed.", self._name);
                } else {
                    println!("io_comp_channel is successfully manually dropped.");
                }
            }

            unsafe {
                println!("Deregister MR.");
                let rc = rdma_binding::ibv_dereg_mr(self.buffer_mr);
                if rc != 0 {
                    eprintln!("{}: ibv_dereg_mr() failed.", self._name)
                } else {
                    println!("MR is successfully manually dropped.");
                }
            }

            unsafe {
                println!("Dropping PD.");
                let rc = rdma_binding::ibv_dealloc_pd(self.pd);
                if rc != 0 {
                    eprintln!("{}: ibv_dealloc_pd() failed.", self._name)
                } else {
                    println!("pd is successfully manually dropped.");
                }
            }

            unsafe {
                println!("Dropping cm_id.");
                rdma_binding::rdma_disconnect(self.cm_id);
                let rc = rdma_binding::rdma_destroy_id(self.cm_id);
                if rc != 0 {
                    eprintln!("{}: rdma_destroy_id() failed.", self._name);
                } else {
                    println!("cm_id is successfully manually dropped.");
                }
            }
        }
    }

    unsafe impl Send for ClientRdmaContext {} // Dangerous hack
}

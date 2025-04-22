pub mod rdma_work_manager {
    use crate::rdma::capsule::capsule::RequestCapsuleContext;
    use crate::rdma::rdma_common::rdma_binding;
    use crate::rdma::ring_buffer::RingBuffer;
    use crate::debug_println_verbose;
    use libc::{c_int, c_uint};
    use std::collections::VecDeque;
    use std::error::Error;
    use std::{fmt, mem, ptr};

    #[derive(Debug)]
    pub enum WorkManagerError {
        InvalidWrId(u16),
        FailedWorkIDAllocation,
        FailedWorkIDFree,
        OperationFailed(String),
        FailedBufferAllocation,
    }
    impl fmt::Display for WorkManagerError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                WorkManagerError::InvalidWrId(wr_d) => write!(f, "Invalid WR ID: {}", wr_d),
                WorkManagerError::FailedWorkIDAllocation => {
                    write!(f, "Failed to allocate RDMA work ID.")
                }
                WorkManagerError::FailedWorkIDFree => write!(f, "Failed to free RDMA work ID."),
                WorkManagerError::OperationFailed(msg) => write!(f, "Operation failed: {}", msg),
                WorkManagerError::FailedBufferAllocation => {
                    write!(f, "Failed to allocate local buffer.")
                }
            }
        }
    }
    impl Error for WorkManagerError {}

    struct WrIdAllocator {
        free_list: VecDeque<u16>,
        max_wr_id: u16,
    }

    impl WrIdAllocator {
        fn new(max_wr_id: u16) -> Self {
            let mut free_list = VecDeque::with_capacity(max_wr_id as usize);
            for i in 0..max_wr_id {
                free_list.push_back(i);
            }

            Self {
                free_list,
                max_wr_id,
            }
        }

        fn allocate_wr_id(&mut self) -> Option<u16> {
            self.free_list.pop_front()
        }

        fn release_wr_id(&mut self, wr_id: u16) -> Result<(), WorkManagerError> {
            if self.free_list.len() == self.max_wr_id as usize {
                return Err(WorkManagerError::FailedWorkIDFree);
            }
            self.free_list.push_back(wr_id);
            Ok(())
        }

        fn any_inflight_wr(&self) -> bool {
            self.free_list.len() != self.max_wr_id as usize
        }
    }

    const MAX_COMPLETION_EVENT: u16 = 64;
    pub struct RdmaWorkManager {
        wr_id_allocator: WrIdAllocator,
        received_wcs: [rdma_binding::ibv_wc; MAX_COMPLETION_EVENT as usize],
        n_completed_work: u16,
        first_unprocessed_wc_index: u16,
        ring_buffer: RingBuffer,
    }

    impl RdmaWorkManager {
        pub fn new(max_wr_id: u16) -> Self {
            Self {
                wr_id_allocator: WrIdAllocator::new(max_wr_id),
                received_wcs: unsafe { mem::zeroed() },
                n_completed_work: 0,
                first_unprocessed_wc_index: 0,
                ring_buffer: RingBuffer::new(max_wr_id as usize),
            }
        }

        pub fn post_rcv_resp_work(
            &mut self,
            wr_id: u16,
            qp: *mut rdma_binding::ibv_qp,
            sge: *mut rdma_binding::ibv_sge,
        ) -> Result<u16, WorkManagerError> {
            let mut bad_client_recv_wr: *mut rdma_binding::ibv_recv_wr = ptr::null_mut();
            self.ring_buffer.insert(wr_id);
            assert!(!sge.is_null());

            let mut wr: rdma_binding::ibv_recv_wr = rdma_binding::ibv_recv_wr {
                wr_id: wr_id as u64,
                next: ptr::null_mut(),
                sg_list: sge,
                num_sge: 1,
            };

            unsafe {
                debug_println_verbose!("post_rcv_resp_work: call ibv_post_recv_ex");
                let ret = rdma_binding::ibv_post_recv_ex(qp, &mut wr, &mut bad_client_recv_wr);
                if ret != 0 {
                    return Err(WorkManagerError::OperationFailed(
                        "Failed to post rcv work".into(),
                    ));
                }
                debug_println_verbose!("[SUCCESS] post_rcv_resp_work: call ibv_post_recv_ex");
            }

            // self.request_for_notification(cq)?;
            Ok(wr_id)
        }

        pub fn post_rcv_req_work(
            &mut self,
            wr_id: u16,
            qp: *mut rdma_binding::ibv_qp,
            sge: *mut rdma_binding::ibv_sge,
        ) -> Result<u16, WorkManagerError> {
            let mut bad_client_recv_wr: *mut rdma_binding::ibv_recv_wr = ptr::null_mut();
            self.ring_buffer.insert(wr_id);
            assert!(!sge.is_null());

            let mut wr: rdma_binding::ibv_recv_wr = rdma_binding::ibv_recv_wr {
                wr_id: wr_id as u64,
                next: ptr::null_mut(),
                sg_list: sge,
                num_sge: 1,
            };

            unsafe {
                let ret = rdma_binding::ibv_post_recv_ex(qp, &mut wr, &mut bad_client_recv_wr);
                if ret != 0 {
                    return Err(WorkManagerError::OperationFailed(
                        "Failed to post rcv work".into(),
                    ));
                }
            }

            // self.request_for_notification(cq)?;

            Ok(wr_id)
        }

        pub fn post_send_response_work(
            &self,
            wr_id: u16,
            qp: *mut rdma_binding::ibv_qp,
            sge: *mut rdma_binding::ibv_sge,
        ) -> Result<(), WorkManagerError> {
            let mut bad_client_send_wr: *mut rdma_binding::ibv_send_wr = ptr::null_mut();

            let mut wr: rdma_binding::ibv_send_wr = rdma_binding::ibv_send_wr {
                wr_id: wr_id as u64,
                next: ptr::null_mut(),
                sg_list: sge,
                num_sge: 1,
                opcode: rdma_binding::ibv_wr_opcode_IBV_WR_SEND,
                send_flags: rdma_binding::ibv_send_flags_IBV_SEND_SIGNALED,
                __bindgen_anon_1: unsafe { mem::zeroed() },
                wr: unsafe { mem::zeroed() },
                qp_type: unsafe { mem::zeroed() },
                __bindgen_anon_2: unsafe { mem::zeroed() },
            };

            unsafe {
                debug_println_verbose!(
                    "post_send_response_work: call ibv_post_send_ex. wr_id: {}",
                    wr_id
                );
                let ret = rdma_binding::ibv_post_send_ex(qp, &mut wr, &mut bad_client_send_wr);
                if ret != 0 {
                    return Err(WorkManagerError::OperationFailed(
                        "Failed to post send response work".into(),
                    ));
                }
                debug_println_verbose!(
                    "[SUCCESS] post_send_response_work: call ibv_post_send_ex. wr_id: {}",
                    wr_id
                );
            }

            // self.request_for_notification(cq)?;

            Ok(())
        }

        pub fn post_send_request_work(
            &self,
            wr_id: u16,
            qp: *mut rdma_binding::ibv_qp,
            sge: *mut rdma_binding::ibv_sge,
        ) -> Result<(), WorkManagerError> {
            let mut bad_client_send_wr: *mut rdma_binding::ibv_send_wr = ptr::null_mut();
            let mut wr: rdma_binding::ibv_send_wr = rdma_binding::ibv_send_wr {
                wr_id: wr_id as u64,
                next: ptr::null_mut(),
                sg_list: sge,
                num_sge: 1,
                opcode: rdma_binding::ibv_wr_opcode_IBV_WR_SEND,
                send_flags: rdma_binding::ibv_send_flags_IBV_SEND_SIGNALED,
                __bindgen_anon_1: unsafe { mem::zeroed() },
                wr: unsafe { mem::zeroed() },
                qp_type: unsafe { mem::zeroed() },
                __bindgen_anon_2: unsafe { mem::zeroed() },
            };

            unsafe {
                debug_println_verbose!(
                    "post_send_request_work: call ibv_post_send_ex. wr_id: {}",
                    wr_id
                );
                let ret = rdma_binding::ibv_post_send_ex(qp, &mut wr, &mut bad_client_send_wr);
                if ret != 0 {
                    return Err(WorkManagerError::OperationFailed(format!(
                        "ibv_post_recv_ex failed with error code: {}",
                        ret
                    )));
                }
                debug_println_verbose!(
                    "[SUCCESS] post_send_request_work: call ibv_post_send_ex. wr_id: {}",
                    wr_id
                );
            }

            // self.request_for_notification(cq)?;

            Ok(())
        }

        pub fn post_rmt_work(
            &self,
            wr_id: u16,
            qp: *mut rdma_binding::ibv_qp,
            local_buffer_ptr: *mut u8,
            local_buffer_lkey: u32,
            remote_addr: u64,
            remote_rkey: u32,
            data_len: u32,
            mode: rdma_binding::ibv_wr_opcode,
        ) -> Result<(), WorkManagerError> {
            let mut local_sge = rdma_binding::ibv_sge {
                addr: local_buffer_ptr as u64,
                length: data_len,
                lkey: local_buffer_lkey,
            };

            let mut remote_wr = rdma_binding::ibv_send_wr {
                wr_id: wr_id as u64,
                next: ptr::null_mut(),
                sg_list: &mut local_sge,
                num_sge: 1,
                opcode: mode,
                send_flags: rdma_binding::ibv_send_flags_IBV_SEND_SIGNALED,
                __bindgen_anon_1: unsafe { mem::zeroed() },
                wr: rdma_binding::ibv_send_wr__bindgen_ty_2 {
                    rdma: rdma_binding::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
                        remote_addr,
                        rkey: remote_rkey,
                    },
                },
                qp_type: unsafe { mem::zeroed() },
                __bindgen_anon_2: unsafe { mem::zeroed() },
            };

            let mut bad_client_send_wr: *mut rdma_binding::ibv_send_wr = ptr::null_mut();
            unsafe {
                // let mode_str = CStr::from_ptr(rdma_binding::ibv_wr_opcode_str(mode))
                //     .to_string_lossy() // converts to Cow<str>, handles invalid UTF-8 safely
                //     .into_owned();
                // debug_println_verbose!("post_rmt_work: call ibv_post_send_ex. wr_id: {}, mode: {}. local_addr: {}, local_lkey: {}, remote_addr: {}, data_len: {}, remote_rkey: {}"
                //     , wr_id, mode_str, local_buffer_ptr as u64, local_buffer_lkey, remote_addr, data_len, remote_rkey);
                let ret =
                    rdma_binding::ibv_post_send_ex(qp, &mut remote_wr, &mut bad_client_send_wr);
                if ret != 0 {
                    return Err(WorkManagerError::OperationFailed(format!(
                        "ibv_post_send_ex failed with error code: {}",
                        ret
                    )));
                }
                // let mode_str = CStr::from_ptr(rdma_binding::ibv_wr_opcode_str(mode))
                //     .to_string_lossy() // converts to Cow<str>, handles invalid UTF-8 safely
                //     .into_owned();
                // debug_println_verbose!(
                //     "[SUCCESS] post_rmt_work: call ibv_post_send_ex. wr_id: {}, mode: {}",
                //     wr_id,
                //     mode_str
                // );
            }

            // self.request_for_notification(cq)?;

            Ok(())
        }

        fn request_for_notification(
            &self,
            cq: *mut rdma_binding::ibv_cq,
        ) -> Result<(), WorkManagerError> {
            unsafe {
                debug_println_verbose!("poll_completed_works: calling ibv_req_notify_cq");
                rdma_binding::ibv_req_notify_cq_ex(cq, 0);
                debug_println_verbose!("[SUCCESS] poll_completed_works: calling ibv_req_notify_cq");
            }

            Ok(())
        }

        pub fn is_not_empty(&self) -> bool {
            self.n_completed_work > 0
        }

        pub fn poll_completed_works(
            &mut self,
            comp_channel: *mut rdma_binding::ibv_comp_channel,
            cq: *mut rdma_binding::ibv_cq,
        ) -> Result<(), WorkManagerError> {
            assert!(!comp_channel.is_null());
            assert!(!cq.is_null());
            self.first_unprocessed_wc_index = 0;
            self.n_completed_work = 0;

            // first poll
            unsafe {
                debug_println_verbose!("poll_completed_works: calling ibv_poll_cq_ex (first poll)");
                let received_wc_ptr = self.received_wcs.as_mut_ptr();
                assert!(self.received_wcs.len() >= MAX_COMPLETION_EVENT as usize);
                let num_polled = rdma_binding::ibv_poll_cq_ex(
                    cq,                            // the CQ, we got notification for
                    MAX_COMPLETION_EVENT as c_int, // max 64 element
                    received_wc_ptr,
                );
                if num_polled < 0 {
                    return Err(WorkManagerError::OperationFailed(format!(
                        "ibv_poll_cq_ex returns < 0: {}",
                        num_polled
                    )));
                }

                self.n_completed_work = num_polled as u16;
                debug_println!(
                    "[SUCCESS] poll_completed_works: calling ibv_poll_cq_ex. Received WC: {}",
                    num_polled
                );
            }

            if self.n_completed_work > 0 {
                return Ok(());
            }

            // then request for notification
            self.request_for_notification(cq)?;

            // Then, poll again to avoid race condition
            unsafe {
                debug_println_verbose!("poll_completed_works: calling ibv_poll_cq_ex (second poll to avoid race condition)");
                let received_wc_ptr = self.received_wcs.as_mut_ptr();
                assert!(self.received_wcs.len() >= MAX_COMPLETION_EVENT as usize);
                let num_polled = rdma_binding::ibv_poll_cq_ex(
                    cq,                            // the CQ, we got notification for
                    MAX_COMPLETION_EVENT as c_int, // max 64 element
                    received_wc_ptr,
                );
                if num_polled < 0 {
                    return Err(WorkManagerError::OperationFailed(format!(
                        "ibv_poll_cq_ex returns < 0: {}",
                        num_polled
                    )));
                }

                self.n_completed_work = num_polled as u16;
                debug_println!(
                    "[SUCCESS] poll_completed_works: calling ibv_poll_cq_ex. Received WC: {}",
                    num_polled
                );
            }

            if self.n_completed_work > 0 {
                return Ok(());
            }

            // Now, it is safe to wait for notification
            unsafe {
                debug_println_verbose!("poll_completed_works: calling ibv_get_cq_event");
                let mut _cq: *mut rdma_binding::ibv_cq = ptr::null_mut(); // we don't care about the pointed CQ as there is only a single CQ
                let mut cq_ctx: *mut std::ffi::c_void = ptr::null_mut();
                rdma_binding::ibv_get_cq_event(comp_channel, &mut _cq, &mut cq_ctx);
                debug_println_verbose!("[SUCCESS] poll_completed_works: calling ibv_get_cq_event");
            }

            // We get a notification. Now, poll the completion
            unsafe {
                debug_println_verbose!("poll_completed_works: calling ibv_poll_cq_ex");
                let received_wc_ptr = self.received_wcs.as_mut_ptr();
                assert!(self.received_wcs.len() >= MAX_COMPLETION_EVENT as usize);
                let num_polled = rdma_binding::ibv_poll_cq_ex(
                    cq,                            // the CQ, we got notification for
                    MAX_COMPLETION_EVENT as c_int, // max 64 element
                    received_wc_ptr,
                );
                if num_polled < 0 {
                    return Err(WorkManagerError::OperationFailed(format!(
                        "ibv_poll_cq_ex returns < 0: {}",
                        num_polled
                    )));
                }

                self.n_completed_work = num_polled as u16;
                debug_println!(
                    "[SUCCESS] poll_completed_works: calling ibv_poll_cq_ex. Received WC: {}",
                    num_polled
                );
            }

            // ack the completion
            unsafe {
                debug_println_verbose!("poll_completed_works: calling ibv_ack_cq_events");
                rdma_binding::ibv_ack_cq_events(cq, self.n_completed_work as c_uint);
                debug_println_verbose!("[SUCCESS] poll_completed_works: calling ibv_ack_cq_events");
            }

            Ok(())
        }

        pub fn next_wc(&mut self) -> Option<&rdma_binding::ibv_wc> {
            if self.first_unprocessed_wc_index < self.n_completed_work {
                self.first_unprocessed_wc_index = self.first_unprocessed_wc_index + 1;
                return Some(&self.received_wcs[(self.first_unprocessed_wc_index - 1) as usize]);
            }

            None
        }

        pub fn release_wr(&mut self, wr_id: u16) -> Result<(), WorkManagerError> {
            self.wr_id_allocator.release_wr_id(wr_id)
        }

        pub fn allocate_wr_id(&mut self) -> Option<u16> {
            self.wr_id_allocator.allocate_wr_id()
        }

        pub fn any_inflight_wr(&self) -> bool {
            self.wr_id_allocator.any_inflight_wr()
        }
    }
}

use crate::rdma::rdma_common::rdma_binding;
use crate::debug_println_verbose;
use libc::{c_int, c_uint};
use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::mem;
use std::ptr;
use std::sync::Arc;
use crossbeam::queue::ArrayQueue;
use crate::rdma::rdma_common::rdma_common::Sendable;

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
            WorkManagerError::InvalidWrId(wr_id) => write!(f, "Invalid WR ID: {}", wr_id),
            WorkManagerError::FailedWorkIDAllocation => write!(f, "Failed to allocate WR ID"),
            WorkManagerError::FailedWorkIDFree => write!(f, "Failed to free WR ID"),
            WorkManagerError::OperationFailed(msg) => write!(f, "Operation failed: {}", msg),
            WorkManagerError::FailedBufferAllocation => write!(f, "Failed to allocate local buffer"),
        }
    }
}

impl Error for WorkManagerError {}

struct WrIdAllocator {
    free_list: Arc<ArrayQueue<u16>>,
    max_wr_id: u16,
}

impl WrIdAllocator {
    fn new(max_wr_id: u16) -> Self {
        let free_list = Arc::new(ArrayQueue::new(max_wr_id as usize));
        for i in 0..max_wr_id {
            free_list.push(i).unwrap();
        }
        Self { free_list, max_wr_id }
    }

    fn allocate_wr_id(&self) -> Option<u16> {
        self.free_list.pop()
    }

    fn release_wr_id(&self, wr_id: u16) -> Result<(), WorkManagerError> {
        if self.free_list.len() == self.max_wr_id as usize {
            return Err(WorkManagerError::FailedWorkIDFree);
        }
        self.free_list.push(wr_id).unwrap();
        Ok(())
    }

    fn any_inflight_wr(&self) -> bool {
        self.free_list.len() != self.max_wr_id as usize
    }
}

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct WcWrapper(pub rdma_binding::ibv_wc);

impl std::fmt::Debug for WcWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WcWrapper")
            .field("wr_id", &self.0.wr_id)
            .field("status", &self.0.status)
            .field("opcode", &self.0.opcode)
            .finish()
    }
}

const MAX_COMPLETION_EVENT: usize = 64;

pub struct RdmaWorkManager {
    wr_id_allocator: WrIdAllocator,
    received_wcs: Box<[WcWrapper; MAX_COMPLETION_EVENT]>,
    n_completed_work: UnsafeCell<u16>,
    first_unprocessed_wc_index: UnsafeCell<u16>,
}

unsafe impl Send for RdmaWorkManager {}
unsafe impl Sync for RdmaWorkManager {}

impl RdmaWorkManager {
    pub fn new(max_wr_id: u16) -> Self {
        let mut array: [WcWrapper; MAX_COMPLETION_EVENT] = unsafe { mem::zeroed() };
        for elem in &mut array {
            *elem = WcWrapper(unsafe { mem::zeroed() });
        }
        Self {
            wr_id_allocator: WrIdAllocator::new(max_wr_id),
            received_wcs: Box::new(array),
            n_completed_work: UnsafeCell::new(0),
            first_unprocessed_wc_index: UnsafeCell::new(0),
        }
    }

    pub fn allocate_wr_id(&self) -> Option<u16> {
        self.wr_id_allocator.allocate_wr_id()
    }

    pub fn release_wr(&self, wr_id: u16) -> Result<(), WorkManagerError> {
        self.wr_id_allocator.release_wr_id(wr_id)
    }

    pub fn any_inflight_wr(&self) -> bool {
        self.wr_id_allocator.any_inflight_wr()
    }

    pub fn is_not_empty(&self) -> bool {
        unsafe { *self.n_completed_work.get() > 0 }
    }

    pub fn next_wc(&self) -> Option<&rdma_binding::ibv_wc> {
        unsafe {
            let idx = *self.first_unprocessed_wc_index.get();
            let count = *self.n_completed_work.get();
            if idx < count {
                *self.first_unprocessed_wc_index.get() += 1;
                Some(&self.received_wcs[idx as usize].0)
            } else {
                None
            }
        }
    }

    pub fn post_rcv_work(&self,
                         wr_id: u16,
                         qp: Sendable<rdma_binding::ibv_qp>,
                         mut sge: rdma_binding::ibv_sge,
    ) -> Result<(), WorkManagerError> {
        let mut wr: rdma_binding::ibv_recv_wr = rdma_binding::ibv_recv_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
        };
        let mut bad_wr: *mut rdma_binding::ibv_recv_wr = ptr::null_mut();
        unsafe {
            if rdma_binding::ibv_post_recv_ex(qp.as_ptr(), &mut wr, &mut bad_wr) != 0 {
                return Err(WorkManagerError::OperationFailed("ibv_post_recv_ex failed".into()));
            }
        }
        Ok(())
    }

    pub fn post_send_work(&self,
                          wr_id: u16,
                          qp: Sendable<rdma_binding::ibv_qp>,
                          mut sge: rdma_binding::ibv_sge,
                          opcode: rdma_binding::ibv_wr_opcode,
                          remote_addr: u64,
                          remote_rkey: u32,
    ) -> Result<(), WorkManagerError> {
        let mut wr = rdma_binding::ibv_send_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
            opcode,
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
        let mut bad_wr: *mut rdma_binding::ibv_send_wr = ptr::null_mut();
        unsafe {
            if rdma_binding::ibv_post_send_ex(qp.as_ptr(), &mut wr, &mut bad_wr) != 0 {
                return Err(WorkManagerError::OperationFailed("ibv_post_send_ex failed".into()));
            }
        }
        Ok(())
    }

    pub fn poll_completed_works(
        &self,
        comp_channel: Sendable<rdma_binding::ibv_comp_channel>,
        cq: Sendable<rdma_binding::ibv_cq>,
    ) -> Result<(), WorkManagerError> {
        unsafe {
            *self.n_completed_work.get() = 0;
            *self.first_unprocessed_wc_index.get() = 0;
        }

        let poll = |label: &str| -> Result<u16, WorkManagerError> {
            unsafe {
                debug_println_verbose!("poll_completed_works: {}", label);
                let num_polled = rdma_binding::ibv_poll_cq_ex(
                    cq.as_ptr(),
                    MAX_COMPLETION_EVENT as c_int,
                    self.received_wcs.as_ptr() as *mut rdma_binding::ibv_wc,
                );
                if num_polled < 0 {
                    return Err(WorkManagerError::OperationFailed(format!(
                        "ibv_poll_cq_ex returns < 0: {}",
                        num_polled
                    )));
                }
                Ok(num_polled as u16)
            }
        };

        unsafe {
            *self.n_completed_work.get() = poll("first poll")?;
        }
        if unsafe { *self.n_completed_work.get() } > 0 {
            return Ok(());
        }

        self.request_for_notification(cq.as_ptr())?;

        unsafe {
            *self.n_completed_work.get() = poll("second poll")?;
        }
        if unsafe { *self.n_completed_work.get() } > 0 {
            return Ok(());
        }

        unsafe {
            debug_println_verbose!("poll_completed_works: calling ibv_get_cq_event");
            let mut _cq: *mut rdma_binding::ibv_cq = ptr::null_mut();
            let mut _ctx: *mut std::ffi::c_void = ptr::null_mut();
            rdma_binding::ibv_get_cq_event(comp_channel.as_ptr(), &mut _cq, &mut _ctx);
            debug_println_verbose!("[SUCCESS] ibv_get_cq_event");
        }

        unsafe {
            *self.n_completed_work.get() = poll("final poll")?;
            rdma_binding::ibv_ack_cq_events(cq.as_ptr(), *self.n_completed_work.get() as c_uint);
        }

        Ok(())
    }

    fn request_for_notification(
        &self,
        cq: *mut rdma_binding::ibv_cq,
    ) -> Result<(), WorkManagerError> {
        unsafe {
            debug_println_verbose!("poll_completed_works: calling ibv_req_notify_cq");
            rdma_binding::ibv_req_notify_cq_ex(cq, 0);
            debug_println_verbose!("[SUCCESS] ibv_req_notify_cq");
        }
        Ok(())
    }

    pub fn post_rcv_req_work(
        &self,
        wr_id: u16,
        qp: Sendable<rdma_binding::ibv_qp>,
        mut sge: rdma_binding::ibv_sge,
    ) -> Result<u16, WorkManagerError> {
        let mut bad_client_recv_wr: *mut rdma_binding::ibv_recv_wr = ptr::null_mut();

        let mut wr: rdma_binding::ibv_recv_wr = rdma_binding::ibv_recv_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
        };

        unsafe {
            let ret = rdma_binding::ibv_post_recv_ex(qp.as_ptr(), &mut wr, &mut bad_client_recv_wr);
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
        qp: Sendable<rdma_binding::ibv_qp>,
        mut sge: rdma_binding::ibv_sge,
    ) -> Result<(), WorkManagerError> {
        let mut bad_client_send_wr: *mut rdma_binding::ibv_send_wr = ptr::null_mut();

        let mut wr: rdma_binding::ibv_send_wr = rdma_binding::ibv_send_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
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
            let ret = rdma_binding::ibv_post_send_ex(qp.as_ptr(), &mut wr, &mut bad_client_send_wr);
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

    pub fn post_rmt_work(
        &self,
        wr_id: u16,
        qp: Sendable<rdma_binding::ibv_qp>,
        mut sge: rdma_binding::ibv_sge,
        remote_addr: u64,
        remote_rkey: u32,
        mode: rdma_binding::ibv_wr_opcode,
    ) -> Result<(), WorkManagerError> {
        let mut remote_wr = rdma_binding::ibv_send_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
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
            let ret =
                rdma_binding::ibv_post_send_ex(qp.as_ptr(), &mut remote_wr, &mut bad_client_send_wr);
            if ret != 0 {
                return Err(WorkManagerError::OperationFailed(format!(
                    "ibv_post_send_ex failed with error code: {}",
                    ret
                )));
            }
        }

        // self.request_for_notification(cq)?;

        Ok(())
    }

    pub fn post_rcv_resp_work(
        &self,
        wr_id: u16,
        qp: Sendable<rdma_binding::ibv_qp>,
        mut sge: rdma_binding::ibv_sge,
    ) -> Result<u16, WorkManagerError> {
        let mut bad_client_recv_wr: *mut rdma_binding::ibv_recv_wr = ptr::null_mut();

        let mut wr: rdma_binding::ibv_recv_wr = rdma_binding::ibv_recv_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
        };

        unsafe {
            debug_println_verbose!("post_rcv_resp_work: call ibv_post_recv_ex");
            let ret = rdma_binding::ibv_post_recv_ex(qp.as_ptr(), &mut wr, &mut bad_client_recv_wr);
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

    pub fn post_send_request_work(
        &self,
        wr_id: u16,
        qp: Sendable<rdma_binding::ibv_qp>,
        mut sge: rdma_binding::ibv_sge,
    ) -> Result<(), WorkManagerError> {
        let mut bad_client_send_wr: *mut rdma_binding::ibv_send_wr = ptr::null_mut();
        let mut wr: rdma_binding::ibv_send_wr = rdma_binding::ibv_send_wr {
            wr_id: wr_id as u64,
            next: ptr::null_mut(),
            sg_list: &mut sge,
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
            let ret = rdma_binding::ibv_post_send_ex(qp.as_ptr(), &mut wr, &mut bad_client_send_wr);
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
}

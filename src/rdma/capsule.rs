pub mod capsule {

    use crate::cmd::NvmeCommand;
    use crate::rdma::rdma_common::rdma_binding;
    use crate::rdma::rdma_common::rdma_binding::ibv_pd;
    use std::error::Error;
    use std::os::raw::{c_int, c_void};
    use std::{fmt, mem, ptr};
    use std::cell::UnsafeCell;
    use std::sync::{Arc, Mutex};
    use libc::munlock;
    use tracing::span;
    use tracing::Level;

    #[derive(Debug)]
    pub enum RDMACapsuleError {
        FailedRDMAMemoryRegionAllocation(usize, String),
        FailedRDMASGERegionAllocation(usize, String),
        Generic(String),
        InvalidIndex,
    }
    impl fmt::Display for RDMACapsuleError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                RDMACapsuleError::FailedRDMAMemoryRegionAllocation(idx, original_msg) => write!(
                    f,
                    "Failed to allocate MR for capsule[{}]: {}",
                    idx, original_msg
                ),
                RDMACapsuleError::FailedRDMASGERegionAllocation(idx, original_msg) => write!(
                    f,
                    "Failed to allocate SGE for capsule[{}]: {}",
                    idx, original_msg
                ),
                RDMACapsuleError::Generic(message) => write!(
                    f,
                    "message",
                ),
                &RDMACapsuleError::InvalidIndex => write!(
                    f,
                    "Accessing invalid capsules index"
                ),
            }
        }
    }
    impl Error for RDMACapsuleError {}

    #[derive(Clone)]
    #[repr(C)]
    pub struct NVMeCapsule {
        pub(crate) cmd: NvmeCommand,
        pub(crate) data_mr_address: u64,
        pub(crate) data_mr_length: u32,
        pub(crate) data_mr_r_key: u32,
        pub(crate) lba: u64,
        pub(crate) in_capsule_data: bool,
    }

    impl NVMeCapsule {
        pub fn zeroed() -> Self {
            Self {
                cmd: Default::default(),
                data_mr_address: 0,
                data_mr_length: 0,
                data_mr_r_key: 0,
                lba: 0,
                in_capsule_data: false,
            }
        }
    }

    impl fmt::Debug for NVMeCapsule {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.in_capsule_data {
                write!(
                    f,
                    "NVMeCapsule {{ cmd: {:?}, data_mr_length: {}, data_mr_r_key: {}, nvme_address: {} }}",
                    self.cmd,
                    self.data_mr_length,
                    self.data_mr_r_key,
                    self.lba
                )
            } else {
                write!(
                    f,
                    "NVMeCapsule {{ cmd: {:?}, data_mr_address: {:#x}, data_mr_length: {}, data_mr_r_key: {}, nvme_address: {} }}",
                    self.cmd,
                    self.data_mr_address,
                    self.data_mr_length,
                    self.data_mr_r_key,
                    self.lba
                )
            }

        }
    }

    #[derive(Clone)]
    #[repr(C)]
    pub struct NVMeResponseCapsule {
        pub(crate) cmd_id: u16,
        pub(crate) status: i16,
        // later to complete
    }

    impl NVMeResponseCapsule {
        pub fn zeroed() -> Self {
            Self {
                cmd_id: 0,
                status: 0,
            }
        }
    }
    impl fmt::Debug for NVMeResponseCapsule {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "NVMeResponseCapsule {{ cid: {}, status: {} }}",
                self.cmd_id, self.status
            )
        }
    }

    pub struct CapsuleContext {
        cnt: u16,
        pub req_capsules: Vec<NVMeCapsule>,
        resp_capsules: Vec<UnsafeCell<NVMeResponseCapsule>>, // allow unsafe modification
        req_capsule_mr: UnsafeCell<*mut rdma_binding::ibv_mr>,
        resp_capsule_mr: UnsafeCell<*mut rdma_binding::ibv_mr>,
    }
    unsafe impl Send for CapsuleContext {}
    unsafe impl Sync for CapsuleContext {}

    impl CapsuleContext {
        pub fn new(n: u16) -> Result<Self, RDMACapsuleError> {
            let mut req_capsules = vec![NVMeCapsule::zeroed(); n as usize];
            let mut resp_capsules = (0..n)
                .map(|_| UnsafeCell::new(NVMeResponseCapsule::zeroed()))
                .collect::<Vec<_>>();

            let req_capsule_ptr = req_capsules.as_ptr();
            if req_capsule_ptr.is_null() {
                return Err(RDMACapsuleError::Generic("Request capsule buffer is not properly initialized.".to_string()));
            }
            let resp_capsule_ptr = resp_capsules.as_ptr();
            if resp_capsule_ptr.is_null() {
                return Err(RDMACapsuleError::Generic("Response capsule buffer is not properly initialized.".to_string()));
            }

            unsafe {
                if libc::mlock(req_capsule_ptr as *const _, req_capsules.len() * mem::size_of::<NVMeCapsule>()) != 0 {
                    return Err(RDMACapsuleError::Generic("Failed to pin request capsules buffer".to_string()));
                }

                if libc::mlock(resp_capsule_ptr as *const _, resp_capsules.len() * mem::size_of::<NVMeResponseCapsule>()) != 0 {
                    return Err(RDMACapsuleError::Generic("Failed to pin response capsules buffer".to_string()));
                }
            }


            Ok(Self {
                cnt: n,
                req_capsules,
                resp_capsules,
                req_capsule_mr: UnsafeCell::new(ptr::null_mut()),
                resp_capsule_mr: UnsafeCell::new(ptr::null_mut()),
            })
        }

        pub fn register_mr(&self, pd: *mut ibv_pd) -> Result<(), RDMACapsuleError> {
            unsafe {
                *self.req_capsule_mr.get() = unsafe {
                    let addr = self.req_capsules.as_ptr() as *mut c_void;
                    let length = self.req_capsules.len() * mem::size_of::<NVMeCapsule>();
                    rdma_binding::ibv_reg_mr(
                        pd,
                        addr,
                        length,
                        (rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE) as c_int,
                    )
                };

                *self.resp_capsule_mr.get() = unsafe {
                    let addr = self.resp_capsules.as_ptr() as *mut c_void;
                    let length = self.resp_capsules.len() * mem::size_of::<NVMeResponseCapsule>();
                    rdma_binding::ibv_reg_mr(
                        pd,
                        addr,
                        length,
                        (rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE) as c_int,
                    )
                };
            }

            Ok(())
        }

        pub fn get_req_sge(
            &self,
            idx: usize,
        ) -> Result<rdma_binding::ibv_sge, RDMACapsuleError> {
            #[cfg(enable_trace)]
            let span = span!(Level::INFO, "capsule.get_req_sge");
            #[cfg(enable_trace)]
            let _ = span.enter();

            #[cfg(not(disable_assert))]
            assert!(idx < self.cnt as usize);
            let capsule_ptr = unsafe { self.req_capsules.as_ptr().add(idx) } as u64;

            let lkey = unsafe {
                let mr_ptr = *self.req_capsule_mr.get();
                (*mr_ptr).lkey.clone()
            };

            Ok(rdma_binding::ibv_sge {
                addr: capsule_ptr,
                length: size_of::<NVMeCapsule>() as u32,
                lkey,
            })
        }

        pub fn get_resp_sge(
            &self,
            idx: usize,
        ) -> Result<rdma_binding::ibv_sge, RDMACapsuleError> {
            #[cfg(enable_trace)]
            let span = span!(Level::INFO, "capsule.get_resp_sge");
            #[cfg(enable_trace)]
            let _ = span.enter();

            #[cfg(not(disable_assert))]
            assert!(idx < self.cnt as usize);
            let capsule_ptr = unsafe { self.resp_capsules.as_ptr().add(idx) } as u64;
            let lkey = unsafe {
                let mr_ptr = *self.resp_capsule_mr.get();
                (*mr_ptr).lkey.clone()
            };

            Ok(rdma_binding::ibv_sge {
                addr: capsule_ptr,
                length: size_of::<NVMeResponseCapsule>() as u32,
                lkey,
            })
        }

        pub fn set_response_status(
            &self,
            idx: usize,
            status: i16
        ) -> Result<(), RDMACapsuleError> {
            #[cfg(enable_trace)]
            let span = span!(Level::INFO, "capsule.set_response_status");
            #[cfg(enable_trace)]
            let _ = span.enter();

            let c_id = self.req_capsules.get(idx).unwrap().cmd.c_id;
            let resp_capsule = unsafe {
                &mut *self.resp_capsules.get(idx).unwrap().get()
            };
            resp_capsule.cmd_id = c_id;
            resp_capsule.status = status;

            Ok(())
        }

        pub fn get_resp_capsule(
            &self,
            idx: usize,
        ) -> Result<&mut NVMeResponseCapsule, RDMACapsuleError> {
            #[cfg(enable_trace)]
            let span = span!(Level::INFO, "capsule.get_resp_capsule");
            #[cfg(enable_trace)]
            let _ = span.enter();

            self.resp_capsules.get(idx)
                .map(|cell| unsafe { &mut *cell.get() })
                .ok_or(RDMACapsuleError::InvalidIndex)
        }

        pub fn get_request_capsule_content(&self, idx: usize) -> Result<(NvmeCommand, u64, u64, u32, u32, bool), RDMACapsuleError> {
            #[cfg(enable_trace)]
            let span = span!(Level::INFO, "capsule.get_request_capsule_content");
            #[cfg(enable_trace)]
            let _ = span.enter();

            #[cfg(any(debug_mode, debug_mode_verbose))]
            debug_println_verbose!("[CAPSULE] get_request_capsule_content: idx = {}", idx);
            let capsule = self.req_capsules.as_slice().get(idx).unwrap();
            Ok((
                capsule.cmd.clone(),
                capsule.lba.clone(),
                capsule.data_mr_address.clone(),
                capsule.data_mr_length.clone(),
                capsule.data_mr_r_key.clone(),
                capsule.in_capsule_data.clone()
            ))
        }
    }

    impl Drop for CapsuleContext {
        fn drop(&mut self) {
            unsafe {
                let req_capsule_ptr = self.req_capsules.as_ptr();
                let _ = munlock(req_capsule_ptr as *const _, self.req_capsules.len() * mem::size_of::<NVMeCapsule>());
                let resp_capsule_ptr = self.resp_capsules.as_ptr();
                let _ = munlock(resp_capsule_ptr as *const _, self.resp_capsules.len() * mem::size_of::<NVMeResponseCapsule>());
            }
        }
    }
}

pub mod capsule {
    use crate::cmd::NvmeCommand;
    use crate::rdma::rdma_common::rdma_binding;
    use crate::rdma::rdma_common::rdma_binding::ibv_pd;
    use std::error::Error;
    use std::os::raw::{c_int, c_void};
    use std::{fmt, mem, ptr};

    #[derive(Debug)]
    pub enum RDMACapsuleError {
        FailedRDMAMemoryRegionAllocation(usize, String),
        FailedRDMASGERegionAllocation(usize, String),
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
            }
        }
    }
    impl Error for RDMACapsuleError {}

    #[derive(Clone)]
    pub struct NVMeCapsule {
        pub(crate) cmd: NvmeCommand,
        pub(crate) data_mr_address: u64,
        pub(crate) data_mr_length: u32,
        pub(crate) data_mr_r_key: u32,
        pub(crate) nvme_address: u64,
    }

    impl NVMeCapsule {
        pub fn zeroed() -> Self {
            Self {
                cmd: Default::default(),
                data_mr_address: 0,
                data_mr_length: 0,
                data_mr_r_key: 0,
                nvme_address: 0,
            }
        }
    }

    impl fmt::Debug for NVMeCapsule {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "NVMeCapsule {{ cmd: {:?}, data_mr_address: {:#x}, data_mr_length: {}, data_mr_r_key: {}, nvme_address: {} }}",
                self.cmd,
                self.data_mr_address,
                self.data_mr_length,
                self.data_mr_r_key,
                self.nvme_address
            )
        }
    }

    #[derive(Clone)]
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

    pub struct RequestCapsuleContext {
        pub(crate) req_capsules: Vec<NVMeCapsule>,
        pub(crate) resp_capsules: Vec<NVMeResponseCapsule>,
        // Unfortunately, we don't have direct control over the placement of
        //  ibv_mr and ibv_sge objects.
        req_capsule_mrs: Vec<*mut rdma_binding::ibv_mr>,
        resp_capsule_mrs: Vec<*mut rdma_binding::ibv_mr>,
        req_capsule_sges: Vec<rdma_binding::ibv_sge>,
        resp_capsule_sges: Vec<rdma_binding::ibv_sge>,
    }

    impl RequestCapsuleContext {
        pub fn new(pd: *mut ibv_pd, n: u16) -> Result<Box<Self>, RDMACapsuleError> {
            let mut req_capsules = vec![NVMeCapsule::zeroed(); n as usize];
            let mut req_capsule_mrs = vec![ptr::null_mut(); n as usize];
            let mut req_capsule_sges = unsafe { vec![mem::zeroed(); n as usize] };
            let mut resp_capsules = vec![NVMeResponseCapsule::zeroed(); n as usize];
            let mut resp_capsule_mrs = vec![ptr::null_mut(); n as usize];
            let mut resp_capsule_sges = unsafe { vec![mem::zeroed(); n as usize] };

            for idx in 0usize..n as usize {
                unsafe {
                    req_capsule_mrs[idx] = rdma_binding::ibv_reg_mr(
                        pd,
                        &mut req_capsules[idx] as *mut NVMeCapsule as *mut c_void,
                        mem::size_of::<NVMeCapsule>(),
                        rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as c_int,
                    )
                };

                unsafe {
                    req_capsule_sges[idx] = rdma_binding::ibv_sge {
                        addr: (*req_capsule_mrs[idx]).addr as u64,
                        length: size_of::<NVMeCapsule>() as u32,
                        lkey: (*req_capsule_mrs[idx]).lkey,
                    };
                };
            }

            for idx in 0usize..n as usize {
                unsafe {
                    resp_capsule_mrs[idx] = rdma_binding::ibv_reg_mr(
                        pd,
                        &mut resp_capsules[idx] as *mut NVMeResponseCapsule as *mut c_void,
                        mem::size_of::<NVMeResponseCapsule>(),
                        rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as c_int,
                    )
                };

                unsafe {
                    resp_capsule_sges[idx] = rdma_binding::ibv_sge {
                        addr: (*resp_capsule_mrs[idx]).addr as u64,
                        length: size_of::<NVMeResponseCapsule>() as u32,
                        lkey: (*resp_capsule_mrs[idx]).lkey,
                    };
                };
            }

            Ok(Box::new(Self {
                req_capsules,
                resp_capsules,
                req_capsule_mrs,
                resp_capsule_mrs,
                req_capsule_sges,
                resp_capsule_sges,
            }))
        }

        pub fn get_req_sge(
            &mut self,
            idx: usize,
        ) -> Result<*mut rdma_binding::ibv_sge, RDMACapsuleError> {
            Ok(self.req_capsule_sges.get_mut(idx).unwrap())
        }

        pub fn get_resp_sge(
            &mut self,
            idx: usize,
        ) -> Result<*mut rdma_binding::ibv_sge, RDMACapsuleError> {
            Ok(&mut self.resp_capsule_sges[idx])
        }

        pub fn get_capsule_pair(
            &mut self,
            idx: usize,
        ) -> Result<(&mut NVMeCapsule, &mut NVMeResponseCapsule), RDMACapsuleError> {
            Ok((
                self.req_capsules.get_mut(idx).unwrap(),
                self.resp_capsules.get_mut(idx).unwrap(),
            ))
        }
    }
}

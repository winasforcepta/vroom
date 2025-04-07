use crate::rdma::rdma_common::rdma_binding;
use crate::rdma::rdma_common::rdma_common::RdmaTransportError;
use libc::{
    mmap, munmap, size_t, MAP_ANONYMOUS, MAP_FAILED, MAP_HUGETLB, MAP_SHARED,
    PROT_READ, PROT_WRITE,
};
use std::os::raw::{c_int, c_void};
use std::{io, ptr};

#[derive(Clone, Copy)]
pub struct SafeBufferPtr(*mut u8);
unsafe impl Send for SafeBufferPtr {}

impl SafeBufferPtr {
    pub fn new(ptr: *mut u8) -> Self {
        SafeBufferPtr(ptr)
    }
    pub fn get(&self) -> *mut u8 {
        self.0
    }
}

struct HugePageBufferInner {
    base_ptr: SafeBufferPtr,
    total_size: usize,
    block_size: usize,
    free_list: Vec<SafeBufferPtr>,
}

/// A buffer manager that allocates a huge-page region and divides it into blocks.
/// Each block shares the same registered MR.
pub struct BufferManager {
    inner: HugePageBufferInner,
}

impl BufferManager {
    /// Create a new BufferManager.
    ///
    /// - `total_bytes`: total size in bytes (e.g., 2 GB).
    /// - `block_size_bytes`: size of each block (e.g., 512 KB).
    /// - `pd`: a valid Protection Domain.
    ///
    /// # Safety
    /// Using raw pointers and RDMA MRs is inherently unsafe.
    pub fn new(total_bytes: usize, block_size_bytes: usize) -> Result<Self, io::Error> {
        if block_size_bytes == 0 || total_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Sizes must be non-zero",
            ));
        }
        if total_bytes % block_size_bytes != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "total_bytes must be a multiple of block_size_bytes",
            ));
        }

        unsafe {
            // Allocate the huge-page memory region.
            let ptr = mmap(
                ptr::null_mut(),
                total_bytes as size_t,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANONYMOUS | MAP_HUGETLB,
                -1,
                0,
            );
            if ptr == MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            let raw_ptr = ptr as *mut u8;
            let safe_base = SafeBufferPtr::new(raw_ptr);

            let block_count = total_bytes / block_size_bytes;
            let mut free_list = Vec::with_capacity(block_count);

            // Subdivide the huge region into blocks.
            for i in 0..block_count {
                let block_ptr = raw_ptr.add(i * block_size_bytes);
                if block_ptr.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let safe_ptr = SafeBufferPtr::new(block_ptr);
                free_list.push(safe_ptr);
            }

            let inner = HugePageBufferInner {
                base_ptr: safe_base,
                total_size: total_bytes,
                block_size: block_size_bytes,
                free_list,
            };

            Ok(BufferManager { inner })
        }
    }

    /// Allocate a block. Returns None if no blocks remain.
    pub fn allocate(&mut self) -> Option<SafeBufferPtr> {
        let buffer_opt = self.inner.free_list.pop();
        if buffer_opt.is_some() {
            Some(buffer_opt.unwrap())
        } else {
            None
        }
    }

    pub fn register_mr(
        &mut self,
        pd_ptr: *mut rdma_binding::ibv_pd,
    ) -> Result<*mut rdma_binding::ibv_mr, RdmaTransportError> {
        assert!(!pd_ptr.is_null(), "PD should not be null");
        let mr_ptr = unsafe {
            rdma_binding::ibv_reg_mr(
                pd_ptr,
                self.inner.base_ptr.get() as *mut c_void,
                self.inner.total_size as size_t,
                (rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
                    | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_READ
                    | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE)
                    as c_int,
            )
        };

        if mr_ptr.is_null() {
            return Err(RdmaTransportError::FailedResourceInit(
                "Failed to register MR on the huge memory".into(),
            ));
        }

        Ok(mr_ptr)
    }

    /// Free a previously allocated block.
    pub fn free(&mut self, buffer_ptr: SafeBufferPtr) {
        self.inner.free_list.push(buffer_ptr);
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        unsafe {
            // Unmap the huge-page region using the base pointer.
            munmap(
                self.inner.base_ptr.get() as *mut c_void,
                self.inner.total_size,
            );
        }
    }
}

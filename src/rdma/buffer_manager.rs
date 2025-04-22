use crate::rdma::rdma_common::rdma_binding;
use crate::rdma::rdma_common::rdma_common::RdmaTransportError;
use crate::memory::{Dma, DmaSlice};
use libc::{munmap, size_t, };
use std::os::raw::{c_int, c_void};
use std::{io};

pub type BufferManagerIdx = u32;
pub type PhysicalAddrType = usize;

/// BufferManager: pre-allocates hugepage memory and divides into RDMA-capable blocks.
pub struct BufferManager {
    total_size: usize,
    n_blocks: u32,
    block_size: usize,
    buffer: ThreadSafeDmaHandle,
    free_idx_list: Vec<u32>,
    mr: Option<*mut rdma_binding::ibv_mr>
}

impl BufferManager {
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

        let block_count: u32 = (total_bytes / block_size_bytes) as u32;
        let mut free_idx_list = Vec::with_capacity(block_count as usize);
        let buffer: Dma<u8> = Dma::allocate(total_bytes).unwrap();
        let thread_safe_dma = ThreadSafeDmaHandle::from(&buffer);
        for i in 0..block_count { free_idx_list.push(i); }
        debug_println_verbose!("[buffer manager] total_bytes = {} block_size_bytes = {} block count = {}", total_bytes, block_size_bytes, block_count);
        Ok(BufferManager {
            total_size: total_bytes,
            n_blocks: block_count,
            block_size: block_size_bytes,
            buffer: thread_safe_dma,
            free_idx_list,
            mr: None
        })
    }

    pub fn register_mr(
        &mut self,
        pd_ptr: *mut rdma_binding::ibv_pd,
    ) -> Result<(), RdmaTransportError> {
        assert!(!pd_ptr.is_null(), "Protection domain should not be null");

        let mr_ptr = unsafe {
            rdma_binding::ibv_reg_mr(
                pd_ptr,
                self.buffer.virt as *mut c_void,
                self.total_size as size_t,
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

        self.mr = Some(mr_ptr);
        Ok(())
    }

    pub fn get_memory_info(&mut self, idx: BufferManagerIdx) -> (*mut u8, PhysicalAddrType, usize, u32) {
        assert!(idx < self.n_blocks, "get_dma_slice out of index");
        let dma = unsafe { self.buffer.to_dma() };
        let virt = unsafe { dma.virt.add((idx as usize* self.block_size)) };
        let phys = dma.phys + (idx as usize * self.block_size);
        let lkey = unsafe { (*self.mr.unwrap()).lkey };
        (virt, phys, self.block_size, lkey)
    }

    pub fn allocate(&mut self) -> Option<(BufferManagerIdx, *mut rdma_binding::ibv_mr)> {
        debug_println_verbose!("[Buffer manager] self.mr.is_some(): {}", self.mr.is_some());
        assert!(self.mr.is_some(), "RDMA MR is not registered yet");

        if let Some(idx) = self.free_idx_list.pop() {
            Some((idx, self.mr.unwrap()))
        } else {
            debug_println_verbose!("Failed to pop free_idx_list");
            None
        }

    }

    pub fn free(&mut self, idx: BufferManagerIdx) {
        self.free_idx_list.push(idx);
    }

    pub fn get_base_dma(&mut self) -> ThreadSafeDmaHandle {
        self.buffer
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        unsafe {
            if self.mr.is_some() {
                rdma_binding::ibv_dereg_mr(self.mr.unwrap());
            }

            munmap(
                self.buffer.virt as *mut c_void,
                self.total_size,
            );
        }
    }
}

/// We don't do any unsafe operation on the base_ptr
unsafe impl Send for BufferManager {}
unsafe impl Sync for BufferManager {}

#[derive(Debug, Clone, Copy)]
pub struct ThreadSafeDmaHandle {
    pub virt: *mut u8,
    pub phys: usize,
    pub size: usize,
}

impl From<&Dma<u8>> for ThreadSafeDmaHandle {
    fn from(dma: &Dma<u8>) -> Self {
        debug_println_verbose!("[DEBUG TRANSFORM DMA -> ThreadSafeDmaHandle] virt: {}, phy: {}, size: {}", dma.virt as u64, dma.phys as u64, dma.size);
        ThreadSafeDmaHandle {
            virt: dma.virt,
            phys: dma.phys,
            size: dma.size,
        }
    }
}

impl ThreadSafeDmaHandle {
    pub unsafe fn to_dma(&self) -> Dma<u8> {
        debug_println_verbose!("[DEBUG TRANSFORM ThreadSafeDmaHandle -> DMA] virt: {}, phy: {}, size: {}", self.virt as u64, self.phys as u64, self.size);
        Dma::from_raw_parts(self.virt, self.phys, self.size)
    }
}

// SAFETY: You ensure externally that the memory is synchronized or immutable
unsafe impl Send for ThreadSafeDmaHandle {}
unsafe impl Sync for ThreadSafeDmaHandle {}
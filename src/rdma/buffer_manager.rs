use tracing::Level;
use crate::rdma::rdma_common::rdma_binding;
use crate::rdma::rdma_common::rdma_common::{RdmaTransportError, MAX_CLIENT};
use crate::memory::{Dma};
use libc::{munmap, size_t, };
use std::os::raw::{c_int, c_void};
use std::{io, ptr};
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use crossbeam::queue::{ArrayQueue};
use tracing::span;
use crate::rdma::rdma_common::rdma_binding::ibv_mr;

pub type BufferManagerIdx = u32;
pub type PhysicalAddrType = usize;

/// BufferManager: pre-allocates hugepage memory and divides into RDMA-capable blocks.
pub struct BufferManager {
    total_size: usize,
    n_blocks: u32,
    block_size: usize,
    buffer: ThreadSafeDmaHandle,
    free_idx_list: Arc<ArrayQueue<u32>>,
    mr: [AtomicPtr<rdma_binding::ibv_mr>; MAX_CLIENT as usize],
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
        let free_idx_list = Arc::new(ArrayQueue::new(block_count as usize));
        let buffer: Dma<u8> = Dma::allocate(total_bytes).unwrap();
        let thread_safe_dma = ThreadSafeDmaHandle::from(&buffer);
        for i in (0..block_count) { free_idx_list.push(i).unwrap(); }
        #[cfg(any(debug_mode, debug_mode_verbose))]
        debug_println_verbose!("[buffer manager] total_bytes = {} block_size_bytes = {} block count = {}", total_bytes, block_size_bytes, block_count);
        Ok(BufferManager {
            total_size: total_bytes,
            n_blocks: block_count,
            block_size: block_size_bytes,
            buffer: thread_safe_dma,
            free_idx_list,
            mr: std::array::from_fn(|_| AtomicPtr::new(null_mut()))
        })
    }

    pub fn register_mr(
        &self,
        client_idx: usize,
        pd_ptr: *mut rdma_binding::ibv_pd,
    ) -> Result<(), RdmaTransportError> {
        assert!(client_idx < MAX_CLIENT as usize, "Protection domain should not be null");
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

        self.mr[client_idx].store(mr_ptr, Ordering::SeqCst);
        Ok(())
    }

    #[inline(always)]
    pub fn get_memory_info(&self, client_idx: usize, idx: BufferManagerIdx) -> (*mut u8, PhysicalAddrType, usize, u32) {
        #[cfg(enable_trace)]
        let span = span!(Level::INFO, "buffer_manager.get_memory_info");
        #[cfg(enable_trace)]
        let _ = span.enter();

        #[cfg(not(disable_assert))]
        assert!(idx < self.n_blocks, "get_dma_slice out of index");
        let dma = unsafe { self.buffer.to_dma() };
        let virt = unsafe { dma.virt.add((idx as usize* self.block_size)) };
        let phys = dma.phys + (idx as usize * self.block_size);
        let lkey = unsafe { (*self.mr[client_idx].load(Ordering::SeqCst)).lkey };
        (virt, phys, self.block_size, lkey)
    }

    #[inline(always)]
    pub fn allocate(&self, client_idx: usize) -> Option<(BufferManagerIdx, *mut rdma_binding::ibv_mr)> {
        #[cfg(enable_trace)]
        let span = span!(Level::INFO, "buffer_manager.allocate");
        #[cfg(enable_trace)]
        let _ = span.enter();

        #[cfg(not(disable_assert))]
        assert!(!self.mr[client_idx].load(Ordering::SeqCst).is_null(), "RDMA MR is not registered yet");

        if let Some(idx) = self.free_idx_list.pop() {
            #[cfg(any(debug_mode, debug_mode_verbose))]
            debug_println_verbose!("[Buffer manager] block {} is allocated", idx);
            Some((idx, self.mr[client_idx].load(Ordering::SeqCst)))
        } else {
            #[cfg(any(debug_mode, debug_mode_verbose))]
            debug_println_verbose!("Failed to pop free_idx_list");
            None
        }

    }

    #[inline(always)]
    pub fn free(&self, idx: BufferManagerIdx) {
        #[cfg(enable_trace)]
        let span = span!(Level::INFO, "buffer_manager.free");
        #[cfg(enable_trace)]
        let _ = span.enter();
        self.free_idx_list.push(idx).unwrap();
    }

    #[inline(always)]
    pub fn get_base_dma(&self) -> ThreadSafeDmaHandle {
        self.buffer
    }

    #[inline(always)]
    pub fn get_lkey(&self, client_idx: usize) -> Option<u32> {
        #[cfg(enable_trace)]
        let span = span!(Level::INFO, "buffer_manager.get_lkey");
        #[cfg(enable_trace)]
        let _ = span.enter();
        let mr = self.mr[client_idx].load(Ordering::SeqCst);
        if mr == ptr::null_mut() {
            None
        } else {
            unsafe { Some((*mr).lkey) }
        }
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        unsafe {
            for i in 0usize..MAX_CLIENT as usize {
                if self.mr[i].load(Ordering::SeqCst) != null_mut() {
                    rdma_binding::ibv_dereg_mr(self.mr[i].load(Ordering::SeqCst));
                }
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
        #[cfg(any(debug_mode, debug_mode_verbose))]
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
        #[cfg(any(debug_mode, debug_mode_verbose))]
        debug_println_verbose!("[DEBUG TRANSFORM ThreadSafeDmaHandle -> DMA] virt: {}, phy: {}, size: {}", self.virt as u64, self.phys as u64, self.size);
        Dma::from_raw_parts(self.virt, self.phys, self.size)
    }
}

// SAFETY: You ensure externally that the memory is synchronized or immutable
unsafe impl Send for ThreadSafeDmaHandle {}
unsafe impl Sync for ThreadSafeDmaHandle {}
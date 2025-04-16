use crate::rdma::rdma_common::rdma_binding;
use crate::rdma::rdma_common::rdma_common::RdmaTransportError;
use crate::memory::{Dma, DmaChunks, DmaSlice};
use libc::{
    mmap, munmap, size_t, MAP_ANONYMOUS, MAP_FAILED, MAP_HUGETLB, MAP_SHARED,
    PROT_READ, PROT_WRITE,
};
use std::os::raw::{c_int, c_void};
use std::{io, ptr, ops::Range};

/// Single RDMA-capable buffer block
#[derive(Clone, Copy)]
pub struct RdmaBufferBlock {
    pub ptr: *mut u8,         // Virtual address of block
    pub len: usize,           // Block size
    pub base_dma_addr: u64,   // MR base DMA address
    pub base_ptr: *mut u8,    // MR base virtual address
}

impl RdmaBufferBlock {
    pub fn dma_address(&self) -> u64 {
        let offset = self.ptr as usize - self.base_ptr as usize;
        self.base_dma_addr + offset as u64
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

/// Adapter to use RdmaBufferBlock as DmaSlice-compatible input
pub struct RdmaBufferAdapter {
    pub dma: Dma<u8>,
}

impl DmaSlice for RdmaBufferAdapter {
    type Item = RdmaBufferAdapter;

    fn chunks(&self, bytes: usize) -> DmaChunks<u8> {
        self.dma.chunks(bytes)
    }

    fn slice(&self, range: Range<usize>) -> Self::Item {
        assert!(range.end <= self.dma.size, "slice out of bounds");

        RdmaBufferAdapter {
            dma: Dma {
                virt: unsafe { self.dma.virt.add(range.start) },
                phys: self.dma.phys + range.start,
                size: range.end - range.start,
            },
        }
    }
}

impl From<RdmaBufferBlock> for RdmaBufferAdapter {
    fn from(block: RdmaBufferBlock) -> Self {
        Self {
            dma: Dma::from_raw_parts(block.ptr, block.dma_address() as usize, block.len),
        }
    }
}


impl std::fmt::Debug for RdmaBufferBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaBufferBlock")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field("base_dma_addr", &format_args!("{:#x}", self.base_dma_addr))
            .field("base_ptr", &self.base_ptr)
            .finish()
    }
}

/// Inner metadata for BufferManager
struct HugePageBufferInner {
    base_ptr: *mut u8,
    total_size: usize,
    block_size: usize,
    free_list: Vec<RdmaBufferBlock>,
}

/// BufferManager: pre-allocates hugepage memory and divides into RDMA-capable blocks.
pub struct BufferManager {
    inner: HugePageBufferInner,
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

        unsafe {
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

            let base_ptr = ptr as *mut u8;
            let block_count = total_bytes / block_size_bytes;
            let mut free_list = Vec::with_capacity(block_count);

            for i in 0..block_count {
                let block_ptr = base_ptr.add(i * block_size_bytes);
                if block_ptr.is_null() {
                    return Err(io::Error::last_os_error());
                }

                free_list.push(RdmaBufferBlock {
                    ptr: block_ptr,
                    len: block_size_bytes,
                    base_dma_addr: 0, // set after MR registration
                    base_ptr,
                });
            }

            let inner = HugePageBufferInner {
                base_ptr,
                total_size: total_bytes,
                block_size: block_size_bytes,
                free_list,
            };

            Ok(BufferManager { inner })
        }
    }

    pub fn register_mr(
        &mut self,
        pd_ptr: *mut rdma_binding::ibv_pd,
    ) -> Result<*mut rdma_binding::ibv_mr, RdmaTransportError> {
        assert!(!pd_ptr.is_null(), "Protection domain should not be null");

        let mr_ptr = unsafe {
            rdma_binding::ibv_reg_mr(
                pd_ptr,
                self.inner.base_ptr as *mut c_void,
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

        let base_dma_addr = unsafe { (*mr_ptr).addr as u64 };

        for block in &mut self.inner.free_list {
            block.base_dma_addr = base_dma_addr;
        }

        Ok(mr_ptr)
    }

    pub fn allocate(&mut self) -> Option<RdmaBufferBlock> {
        self.inner.free_list.pop()
    }

    pub fn free(&mut self, buffer_block: RdmaBufferBlock) {
        self.inner.free_list.push(buffer_block);
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        unsafe {
            munmap(
                self.inner.base_ptr as *mut c_void,
                self.inner.total_size,
            );
        }
    }
}

/// We don't do any unsafe operation on the base_ptr
unsafe impl Send for BufferManager {}
unsafe impl Sync for BufferManager {}

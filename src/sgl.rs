use crate::memory::{Dma, DmaBufferLike};
use std::error::Error;

#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct SglDescriptor {
    pub address: u64,
    pub length: u32,
    pub rtype: u8,    // renamed from `type` to avoid keyword conflict
    pub subtype: u8,
    pub reserved: [u8; 2],
}

impl SglDescriptor {
    pub fn new_data_block(address: u64, length: u32) -> Self {
        println!("[SGL] new_data_block()");
        println!("  -> address = {:#x}", address);
        println!("  -> length  = {} (0x{:x})", length, length);
        println!("  -> type    = 0x0 (Data Block), subtype = 0x2");

        Self {
            address,
            length,
            rtype: 0x0, // Data Block
            subtype: 0x2, // <-- PHYSICAL address
            reserved: [0; 2],
        }
    }

    pub fn new_segment(address: u64, length: u32) -> Self {
        println!("[SGL] new_segment()");
        println!("  -> address = {:#x}", address);
        println!("  -> length  = {} (0x{:x})", length, length);
        println!("  -> type    = 0x2 (Segment), subtype = 0x2");

        Self {
            address,
            length,
            rtype: 0x2,
            subtype: 0x2,
            reserved: [0; 2],
        }
    }
}

pub struct SglChain {
    pub segment: Dma<u8>,      // Single segment descriptor
    pub blocks: Dma<u8>,       // Pre-allocated block descriptors
    pub scratch: Vec<SglDescriptor>, // Reusable scratch space
    pub data_block_only: bool, // QEMU compatibility mode
}

impl SglChain {
    pub fn preallocate(max_entries: usize, data_block_only: bool) -> Result<Self, Box<dyn Error>> {
        println!("[SGL] preallocate() called: max_entries = {}, data_block_only = {}", max_entries, data_block_only);

        let segment = Dma::allocate(16)?;
        let blocks = if data_block_only {
            Dma::allocate(16)?
        } else {
            Dma::allocate(max_entries * 16)?
        };

        assert_eq!(segment.phys % 16, 0, "Segment not 16-byte aligned");
        assert_eq!(blocks.phys % 16, 0, "Blocks not 16-byte aligned");

        Ok(Self {
            segment,
            blocks,
            scratch: vec![SglDescriptor::default(); max_entries],
            data_block_only,
        })
    }

    pub fn fill_from(
        &mut self,
        base_addr: u64,
        total_len: usize,
        max_chunk: usize,
    ) -> Result<(u64, u32), Box<dyn Error>> {
        println!("[SGL] fill_from() called:");
        println!("  -> base_addr  = {:#x}", base_addr);
        println!("  -> total_len  = {}", total_len);
        println!("  -> max_chunk  = {}", max_chunk);

        if self.data_block_only {
            println!("  -> Using QEMU compatibility mode (data block only)");
            let desc = SglDescriptor::new_data_block(base_addr, total_len as u32);
            unsafe {
                std::ptr::write_volatile(self.segment.virt as *mut SglDescriptor, desc);
            }
            return Ok((self.segment.phys as u64, 16));
        }

        println!("  -> Using full SGL chaining mode");
        let mut remaining = total_len;
        let mut addr = base_addr;
        let mut num_entries = 0;

        while remaining > 0 && num_entries < self.scratch.len() {
            let chunk_size = remaining.min(max_chunk) as u32;
            self.scratch[num_entries] = SglDescriptor::new_data_block(addr, chunk_size);
            println!("    [{}] Added chunk: addr={:#x}, size={}", num_entries, addr, chunk_size);
            addr += chunk_size as u64;
            remaining -= chunk_size as usize;
            num_entries += 1;
        }

        if num_entries == 0 {
            println!("  -> ERROR: No SGL entries created");
            return Err("No SGL entries created".into());
        }

        if num_entries == 1 {
            println!("  -> Single descriptor optimization");
            unsafe {
                std::ptr::write_volatile(self.segment.virt as *mut SglDescriptor, self.scratch[0]);
            }
            return Ok((self.segment.phys as u64, 16));
        }

        println!("  -> Copying {} descriptors into block list", num_entries);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.scratch.as_ptr(),
                self.blocks.virt as *mut SglDescriptor,
                num_entries,
            );

            let seg_desc = SglDescriptor::new_segment(
                self.blocks.phys as u64,
                (num_entries * 16) as u32,
            );
            println!("  -> Segment descriptor: {:?}", seg_desc);
            std::ptr::write_volatile(self.segment.virt as *mut SglDescriptor, seg_desc);
        }

        Ok((self.segment.phys as u64, 16))
    }
}

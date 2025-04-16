use std::error::Error;
use std::{env, process};
use vroom::memory::*;
use vroom::sgl::SglChain;

const SGL_DATA_BLOCK_ONLY: bool = false; // Set to false for real NVMe targets

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example hello_world_sgl <pci bus id>");
            process::exit(1);
        }
    };

    println!("[Main] Initializing NVMe controller for PCI address: {}", pci_addr);
    let mut nvme = vroom::init(&pci_addr)?;

    // Write operation
    println!("[Main] Allocating DMA buffer for write");
    let mut write_buf = Dma::allocate(512)?;
    let data = "hello world".as_bytes();
    write_buf[0..data.len()].copy_from_slice(data);

    println!("[Main] Sending write_sgl command");
    println!("  -> SGL mode: {}", if SGL_DATA_BLOCK_ONLY { "Data Block Only (QEMU)" } else { "Segmented SGL" });
    println!("  -> Data to write: {:?}", std::str::from_utf8(data)?);
    nvme.write_sgl(&write_buf.slice(0..512), 0, SGL_DATA_BLOCK_ONLY)?;

    // Read operation
    println!("[Main] Allocating DMA buffer for read");
    let mut read_buf: vroom::memory::Dma<u8> = Dma::allocate(512)?;

    println!("[Main] Sending read_sgl command");
    nvme.read_sgl(&read_buf.slice(0..512), 0, SGL_DATA_BLOCK_ONLY)?;

    println!("[Main] Copying data from read buffer...");
    let result = unsafe { std::slice::from_raw_parts(read_buf.virt, data.len()) };
    println!("Read back: {}", std::str::from_utf8(result)?);

    Ok(())
}

#![feature(stdsimd)]

#[allow(unused)]
mod cmd;
#[allow(dead_code)]
mod memory;
mod nvme;
#[allow(dead_code)]
mod pci;
#[allow(dead_code)]
mod queues;

use nvme::NvmeDevice;
use self::pci::*;
use std::error::Error;

use std::time::Instant;
use std::io::Read;
use std::fs::File;

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(crate) unsafe fn pause() {
    std::arch::aarch64::__yield();
}

#[cfg(target_arch = "x86")]
#[inline(always)]
pub(crate) unsafe fn pause() {
    std::arch::x86::_mm_pause();
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) unsafe fn pause() {
    std::arch::x86_64::_mm_pause();
}

pub fn init(pci_addr: &str) -> Result<(), Box<dyn Error>> {
    let mut vendor_file = pci_open_resource_ro(pci_addr, "vendor").expect("wrong pci address");
    let mut device_file = pci_open_resource_ro(pci_addr, "device").expect("wrong pci address");
    let mut config_file = pci_open_resource_ro(pci_addr, "config").expect("wrong pci address");

    let _vendor_id = read_hex(&mut vendor_file)?;
    let _device_id = read_hex(&mut device_file)?;
    let class_id = read_io32(&mut config_file, 8)? >> 16;
    // println!("{:X}", class_id);

    // 0x01 -> mass storage device class id
    // 0x08 -> nvme subclass
    if class_id != 0x0108 {
        return Err(format!("device {} is not a block device", pci_addr).into());
    }

    // todo: init device
    let mut nvme = NvmeDevice::init(pci_addr)?;

    nvme.identify_controller()?;
    nvme.create_io_queue_pair()?;
    let ns = nvme.identify_namespace_list(0);

    for n in ns {
        println!("ns_id: {n}");
        nvme.identify_namespace(n);
    }

    // Testing stuff
    let n = 1000;
    let blocks = 24;
    let mut lba = 0;
    let mut read = std::time::Duration::new(0, 0);
    let mut write = std::time::Duration::new(0, 0);

    let mut shakespeare = File::open("pg100.txt")?;
    let mut buffer = Vec::new();
    shakespeare.read_to_end(&mut buffer)?;
    let before = Instant::now();
    nvme.write_raw(&buffer[..], lba)?;
    write += before.elapsed();

    // for _ in 0..n {
    //     // read
    //     let before = Instant::now();

    //     // this guy doesn't work when writing more than 2 pages??
    //     nvme.read(1, blocks, lba);
    //     read += before.elapsed();
    //     // println!("{blocks} block read: {:?}", before.elapsed());
    //     let rand_block = &(0.. (512 * blocks)).map(|_| { rand::random::<u8>() }).collect::<Vec<_>>()[..];

    //     assert_eq!(rand_block.len(), 512 * blocks as usize);

    //     // write
    //     let before = Instant::now();
    //     nvme.write_raw(rand_block, lba)?;
    //     write += before.elapsed();
    //     // println!("{blocks} block write: {:?}", before.elapsed());
    //     let data = nvme.read_tmp(1, blocks, lba);
    //     assert_eq!(data, rand_block);

    //     lba += blocks as u64;
    //     // nvme.read(1, 4);
    // }

    // println!("{blocks} block read: {:?}", read / n);
    println!("{blocks} block write: {:?}", write / n);

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct NvmeNamespace {
    pub id: u32,
    pub blocks: u64,
    pub block_size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct NvmeStats {
    pub completions: u64,
    pub submissions: u64,
}

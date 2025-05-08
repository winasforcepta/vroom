#![feature(slice_pattern)]
#![cfg_attr(target_arch = "aarch64", feature(stdarch_arm_hints))]
extern crate core;

#[allow(unused)]
mod cmd;
#[allow(dead_code)]
pub mod memory;
#[allow(dead_code)]
mod nvme;
#[allow(dead_code)]
mod pci;
#[allow(dead_code)]
mod queues;
#[allow(unused)]
pub mod rdma;

pub use memory::HUGE_PAGE_SIZE;
pub use nvme::{NvmeDevice, NvmeQueuePair};
use pci::*;
pub use queues::QUEUE_LENGTH;
use std::error::Error;

pub fn init(pci_addr: &str) -> Result<NvmeDevice, Box<dyn Error>> {
    let mut vendor_file = pci_open_resource_ro(pci_addr, "vendor").expect("wrong pci address");
    let mut device_file = pci_open_resource_ro(pci_addr, "device").expect("wrong pci address");
    let mut config_file = pci_open_resource_ro(pci_addr, "config").expect("wrong pci address");

    let _vendor_id = read_hex(&mut vendor_file)?;
    let _device_id = read_hex(&mut device_file)?;
    let class_id = read_io32(&mut config_file, 8)? >> 16;

    // 0x01 -> mass storage device class id
    // 0x08 -> nvme subclass
    if class_id != 0x0108 {
        return Err(format!("device {} is not a block device", pci_addr).into());
    }

    let mut nvme = NvmeDevice::init(pci_addr)?;
    nvme.identify_controller()?;
    let ns = nvme.identify_namespace_list(0);
    for n in ns {
        println!("ns_id: {n}");
        nvme.identify_namespace(n);
    }
    Ok(nvme)
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

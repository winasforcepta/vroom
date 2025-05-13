use clap::Parser;
use std::net::IpAddr;
use vroom::rdma::rdma_target::rdma_target::RdmaTarget;

#[derive(Parser, Debug)]
#[command(name = "rdma-server")]
#[command(about = "RDMA Server that binds to a specific IP and allocates memory")]
struct Args {
    /// IP address to bind to
    #[arg(long)]
    ip: IpAddr,

    /// Device PCI address
    #[arg(long)]
    pci_addr: String,

    /// Reserved memory size in GB (default: 2)
    #[arg(long = "memory-gb", default_value_t = 2)]
    memory: usize,

    /// Block size in bytes
    #[arg(long = "block-size", default_value_t = 512)]
    block_size: usize,

    #[arg(long = "queue-depth", default_value_t = 128)]
    queue_depth: usize,
}
fn read() {
    // println!("read data from NVMe ctrl");
}

fn write() {
    // println!("write data from NVMe ctrl");
}
fn main() {
    let args = Args::parse();

    let ip = args.ip;
    let reserved_bytes = args.memory * 1024 * 1024 * 1024; // Convert GB to bytes
    let block_size = args.block_size;
    let pci_addr = args.pci_addr;

    let ipv4 = match ip {
        IpAddr::V4(addr) => addr,
        IpAddr::V6(_) => {
            eprintln!("IPv6 is not supported in this setup.");
            std::process::exit(1);
        }
    };

    let mut target = RdmaTarget::new(ipv4, reserved_bytes, block_size, &pci_addr, args.queue_depth.clone()).expect("Failed to create RDMA target");
    println!("Server is listening");
    target.run().expect("Fails to start RDMA target.");
}

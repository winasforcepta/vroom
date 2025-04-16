use clap::Parser;
use std::net::IpAddr;
use std::thread;
use std::time::Duration;
use vroom::debug_println;
use vroom::rdma::buffer_manager::BufferManager;
use vroom::rdma::rdma_initiator::rdma_initiator::RdmaInitiator;

#[derive(Parser, Debug)]
#[command(name = "rdma-server")]
#[command(about = "RDMA Server that binds to a specific IP and allocates memory")]
struct Args {
    #[arg(long = "server")]
    ip: IpAddr,
    #[arg(long, default_value_t = 1)]
    read: usize,
    #[arg(long, default_value_t = 1)]
    write: usize,
}

pub static DEBUG_MODE: bool = true;
fn main() {
    let args = Args::parse();
    let ip = args.ip;
    let n_read = args.read;
    let n_write = args.write;

    let ipv4 = match ip {
        IpAddr::V4(addr) => addr,
        IpAddr::V6(_) => {
            eprintln!("IPv6 is not supported in this setup.");
            std::process::exit(1);
        }
    };

    let mut buffer_manager = BufferManager::new(2_147_483_648usize / 4, 512usize).unwrap();
    let mut transport = RdmaInitiator::connect(ipv4, 4421, &mut buffer_manager)
        .expect("failed to connect to server and create transport.");
    thread::sleep(Duration::from_secs(1));
    let mut n_successes = 0;
    let mut n_errors = 0;
    let size = 64;
    let mut s = String::with_capacity(size);
    s.push_str(&"abcd".repeat(size / 4));
    let mut cid = 0u16;
    let nvme_addr = 0u64;
    let mut outstanding_requests = 0usize;

    let mut write_buffer = buffer_manager.allocate().unwrap();
    let mut read_buffer_ctx = buffer_manager.allocate().unwrap();

    for _i in 0..n_write {
        transport
            .post_remote_io_write(cid, nvme_addr, &mut write_buffer, 512u32)
            .expect("failed to post remote_io_write");
        cid = cid + 1;
        outstanding_requests = outstanding_requests + 1;
    }

    for _i in 0..n_read {
        transport
            .post_remote_io_read(cid, nvme_addr, &mut read_buffer_ctx, 512u32)
            .expect("failed to post remote_io_read");
        cid = cid + 1;
        outstanding_requests = outstanding_requests + 1;
    }

    while outstanding_requests > 0 {
        let (ns, nf) = transport.poll_completions().unwrap();
        n_successes += ns as usize;
        n_errors += nf as usize;
        outstanding_requests -= (ns + nf) as usize;
        debug_println!(
            "received {} finished I/O operations. {} remaining.",
            ns + nf,
            outstanding_requests
        );
    }

    println!("Finshed. success: {}. errors: {}.", n_successes, n_errors);
}

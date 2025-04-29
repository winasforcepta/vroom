use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::ops::Add;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, thread};
use std::time::{Duration, Instant};
use vroom::rdma::buffer_manager::BufferManager;
use vroom::rdma::rdma_initiator::rdma_initiator::RdmaInitiator;
use vroom::HUGE_PAGE_SIZE;

#[derive(ValueEnum, Debug, Clone, PartialEq)]
enum IOMode {
    Read,
    Write,
    Mixed,
}

impl fmt::Display for IOMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IOMode::Read => "Read",
            IOMode::Write => "Write",
            IOMode::Mixed => "Mixed",
        };
        write!(f, "{}", s)
    }
}

#[derive(ValueEnum, Debug, Clone, PartialEq)]
enum Workload {
    Sequential,
    Random
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Workload::Sequential => "Sequential",
            Workload::Random => "Random",
        };
        write!(f, "{}", s)
    }
}


#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long = "server")]
    ip: IpAddr,

    #[arg(long, default_value_t = Workload::Sequential)]
    workload: Workload,

    #[arg(long, default_value_t = 10)]
    duration_seconds: u64,

    #[arg(long, default_value_t = 1)]
    queue_depth: u32,

    #[arg(long, default_value_t = 4096)]
    block_size: u32,

    #[arg(long, default_value_t = IOMode::Write)]
    mode: IOMode,

    #[arg(long, default_value_t = 17_179_869_184)] // 16GB default
    ns_size_bytes: u64,
}

pub struct Semaphore {
    permits: AtomicUsize,
}

impl Semaphore {
    pub fn new(initial_permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(initial_permits),
        }
    }

    pub fn acquire(&self) {
        loop {
            let available = self.permits.load(Ordering::Acquire);
            if available > 0 {
                if self.permits
                    .compare_exchange(available, available - 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            } else {
                thread::yield_now();
            }
        }
    }

    pub fn try_acquire(&self) -> bool {
        let available = self.permits.load(Ordering::Acquire);
        if available > 0 {
            self.permits
                .compare_exchange(available, available - 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        } else {
            false
        }
    }

    pub fn release(&self, n: usize) {
        self.permits.fetch_add(n, Ordering::Release);
    }
}

fn generate_lba_offsets(ns_size_bytes: u64, block_size: u64, random: bool) -> Vec<u64> {
    let num_blocks = ns_size_bytes / block_size;
    let mut rng = thread_rng();

    if random {
        // Generate all blocks in random order
        let mut lbas: Vec<u64> = (0..num_blocks).map(|i| i * block_size).collect();
        lbas.shuffle(&mut rng);
        lbas
    } else {
        // Generate sequentially
        (0..num_blocks).collect()
    }
}

fn generate_mode_is_write(ns_size_bytes: u64, block_size: u64, io_mode: IOMode) -> Vec<bool> {
    let num_blocks = ns_size_bytes / block_size;
    let mut rng = thread_rng();

    let ret = match io_mode {
        IOMode::Read => {
            (0..num_blocks).map(|i| true).collect()
        }
        IOMode::Write => {
            (0..num_blocks).map(|i| false).collect()
        }
        IOMode::Mixed => {
            let mut generated: Vec<bool> = (0..num_blocks).map(|i| if i % 2 == 0 { false } else { true }).collect();
            generated.shuffle(&mut rng);
            generated
        }
    };

    ret
}

fn print_result(bandwidth: f64, io_per_sec: f64, latency_min: u64, latency_percentile_25: u64,
                latency_percentile_50: u64, latency_percentile_75: u64, latency_percentile_90: u64,
                latency_percentile_99: u64, latency_max: u64) -> () {
    println!("\"bandwidth\", \"io_per_sec\", \"latency_min\", \"latency_percentile_25\", \"latency_percentile_50\", \"latency_percentile_75\", \"latency_percentile_90\", \"latency_percentile_99\", \"latency_max\"");
    println!("\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\"",
             bandwidth, io_per_sec, latency_min, latency_percentile_25, latency_percentile_50,
             latency_percentile_75, latency_percentile_90, latency_percentile_99, latency_max);
}

fn main() {
    let args = Args::parse();
    let semaphore = Semaphore::new(args.queue_depth as usize);
    let mut per_io_time_tracker: VecDeque<Instant> = VecDeque::with_capacity(args.queue_depth as usize);
    let mut hist: Histogram<u64> = Histogram::new_with_bounds(1u64, 300_000_000_000u64, 3).unwrap();

    let ip = args.ip;
    let ipv4 = match ip {
        IpAddr::V4(addr) => addr,
        IpAddr::V6(_) => {
            eprintln!("IPv6 is not supported in this setup.");
            std::process::exit(1);
        }
    };
    let mut buffer_manager = BufferManager::new(HUGE_PAGE_SIZE, args.block_size as usize).unwrap();
    let mut transport = RdmaInitiator::connect(ipv4, 4421)
        .expect("failed to connect to server and create transport.");
    thread::sleep(Duration::from_secs(1));
    let lbas = generate_lba_offsets(args.ns_size_bytes as u64, args.block_size as u64, args.workload == Workload::Random);
    let io_write_mode = generate_mode_is_write(args.ns_size_bytes as u64, args.block_size as u64, args.mode);

    let (read_io_buffer_idx, read_buffer_mr) = buffer_manager.allocate().unwrap();
    let (read_io_buffer, _, _, _) = buffer_manager.get_memory_info(read_io_buffer_idx);
    let read_io_buffer_rkey = unsafe { (*read_buffer_mr).rkey };

    let (write_io_buffer_idx, write_buffer_mr) = buffer_manager.allocate().unwrap();
    let (write_io_buffer, _, _, _) = buffer_manager.get_memory_info(write_io_buffer_idx);
    let write_io_buffer_rkey = unsafe { (*write_buffer_mr).rkey };

    let duration = Duration::from_secs(args.duration_seconds);
    let mut total = Duration::ZERO;
    let mut total_io = 0;
    let mut step = 0usize;

    while total < duration {
        let before = Instant::now();
        while semaphore.try_acquire() {
            let lba = lbas[step];
            let is_write_mode = io_write_mode[step];
            per_io_time_tracker.push_back(Instant::now());
            match is_write_mode {
                true => {
                    transport
                        .post_remote_io_write(step as u16, lba, write_io_buffer, args.block_size, write_io_buffer_rkey)
                        .expect("failed to post remote_io_write");
                }
                false => {
                    transport
                        .post_remote_io_read(step as u16, lba, read_io_buffer, args.block_size, read_io_buffer_rkey)
                        .expect("failed to post remote_io_read");
                }
            }

            step = (step + 1) % lbas.len();
        }

        let (ns, nf) = transport.poll_completions().unwrap();
        semaphore.release((ns + nf) as usize);
        total_io += (ns + nf) as usize;
        for _i in 0..(ns + nf) {
            let latency = per_io_time_tracker.pop_front().unwrap().elapsed().as_nanos() as u64;
            hist.record(latency.max(1)).unwrap() // avoid 0
        }

        let elapsed = before.elapsed();
        total += elapsed;
    }

    let before = Instant::now();
    let (ns, nf) = transport.poll_completions().unwrap();
    total_io += (ns + nf) as usize;

    for _i in 0..(ns + nf) {
        let latency = per_io_time_tracker.pop_front().unwrap().elapsed().as_nanos() as u64;
        hist.record(latency.max(1)).unwrap() // avoid 0
    }

    let elapsed = before.elapsed();
    total += elapsed;


    let actual_runtime_secs = total.as_secs_f64();
    let total_io_bytes = total_io * args.block_size as usize;

    let bandwidth = total_io_bytes as f64 / actual_runtime_secs; // MB/s
    let io_per_sec = total_io as f64 / actual_runtime_secs;
    let latency_min = hist.min(); // nanoseconds
    let latency_percentile_25 = hist.value_at_quantile(0.25); // nanoseconds
    let latency_percentile_50 = hist.value_at_quantile(0.5); // nanoseconds
    let latency_percentile_75 = hist.value_at_quantile(0.75); // nanoseconds
    let latency_percentile_90 = hist.value_at_quantile(0.9); // nanoseconds
    let latency_percentile_99 = hist.value_at_quantile(0.99); // nanoseconds
    let latency_max = hist.max(); // nanoseconds
    print_result(bandwidth, io_per_sec, latency_min, latency_percentile_25,
                 latency_percentile_50, latency_percentile_75, latency_percentile_90,
                 latency_percentile_99, latency_max)
}
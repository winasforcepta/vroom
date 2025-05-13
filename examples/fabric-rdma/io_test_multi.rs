use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use libc::size_t;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::alloc::{alloc, Layout};
use std::collections::VecDeque;
use std::net::IpAddr;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{fmt, thread};
use std::sync::{Arc, Mutex};
use vroom::debug_println_verbose;
use vroom::rdma::rdma_common::rdma_binding;
use vroom::rdma::rdma_initiator::rdma_initiator::RdmaInitiator;

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

    #[arg(long, default_value_t = 1)] // 16GB default
    client: u64,
}

fn generate_lba_offsets(ns_size_bytes: u64, block_size: u64, random: bool) -> Vec<u64> {
    let num_blocks = ns_size_bytes / block_size;
    let mut rng = thread_rng();

    if random {
        // Generate all blocks in random order
        let mut lbas: Vec<u64> = (0..num_blocks).collect();
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
            (0..num_blocks).map(|_i| false).collect()
        }
        IOMode::Write => {
            (0..num_blocks).map(|_i| true).collect()
        }
        IOMode::Mixed => {
            let mut generated: Vec<bool> = (0..num_blocks).map(|i| if i % 2 == 0 { false } else { true }).collect();
            generated.shuffle(&mut rng);
            generated
        }
    };

    ret
}

fn print_result(mode: IOMode, workload: Workload, block_size: u32, queue_depth: u32,
                bandwidth: f64, io_per_sec: f64, latency_min: u64, latency_percentile_25: u64,
                latency_percentile_50: u64, latency_percentile_75: u64, latency_percentile_90: u64,
                latency_percentile_99: u64, latency_max: u64) -> () {
    let mode_str: String = match mode {
        IOMode::Read => { "Read".to_string() },
        IOMode::Write => { "Write".to_string() }
        IOMode::Mixed => { "Mixed".to_string() }
    };
    let workload_str = match workload {
        Workload::Sequential => { "Sequential".to_string() }
        Workload::Random => { "Random".to_string() }
    };
    println!("\"mode\", \"workload\", \"block_size\", \"queue_depth\", \"bandwidth\", \"io_per_sec\", \"latency_min_us\", \"latency_percentile_25_us\", \"latency_percentile_50_us\", \"latency_percentile_75_us\", \"latency_percentile_90_us\", \"latency_percentile_99_us\", \"latency_max_us\"");
    println!("\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\"",
             mode_str, workload_str, block_size, queue_depth,
             bandwidth, io_per_sec, latency_min as f64 / 1000f64, latency_percentile_25 as f64 / 1000f64, latency_percentile_50 as f64 / 1000f64,
             latency_percentile_75 as f64 / 1000f64, latency_percentile_90 as f64 / 1000f64, latency_percentile_99 as f64 / 1000f64, latency_max as f64 / 1000f64);
}



fn main() {
    let args = Args::parse();
    let ip = args.ip;
    let ipv4 = match ip {
        IpAddr::V4(addr) => addr,
        IpAddr::V6(_) => {
            eprintln!("IPv6 is not supported in this setup.");
            std::process::exit(1);
        }
    };

    let mut handles = vec![];
    let mut not_ready_clients = Arc::from(AtomicUsize::new(args.client.clone() as usize));
    let mut connection_mtx = Arc::from(Mutex::new(true));

    for i in 0..args.client {
        let connection_mtx = connection_mtx.clone();
        let ns_size_bytes = args.ns_size_bytes.clone();
        let block_size = args.block_size.clone();
        let workload = args.workload.clone();
        let mode = args.mode.clone();
        let mut per_io_time_tracker: VecDeque<Instant> = VecDeque::with_capacity(args.queue_depth as usize);
        let mut quota = args.queue_depth as usize;
        let duration_seconds = args.duration_seconds.clone();
        let not_ready_clients = not_ready_clients.clone();

        let handle = thread::spawn(move || {
            let mut hist: Histogram<u64> = Histogram::new_with_bounds(1u64, 300_000_000_000u64, 3).unwrap();
            let mut transport = {
                let _guard = connection_mtx.lock().unwrap();
                println!("Client {} is connecting.", i);
                RdmaInitiator::connect(ipv4, 4421, quota)
                    .expect("failed to connect to server and create transport.")
            };
            println!("Client {} is connected.", i);
            let pd = transport.get_pd().expect("failed to get pd");
            thread::sleep(Duration::from_millis(500));
            let lbas = generate_lba_offsets(ns_size_bytes, block_size as u64, workload == Workload::Random);
            let io_write_mode = generate_mode_is_write(ns_size_bytes, block_size as u64, mode);

            let write_io_buffer_layout = Layout::from_size_align(args.block_size as usize, 1).unwrap();
            let write_io_buffer = unsafe { alloc(write_io_buffer_layout) };
            let write_buffer_mr = unsafe {
                rdma_binding::ibv_reg_mr(
                    pd,
                    write_io_buffer as *mut c_void,
                    args.block_size as size_t,
                    (rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
                        | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_READ
                        | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE)
                        as c_int,
                )
            };
            let write_io_buffer_rkey = unsafe { (*write_buffer_mr).rkey };

            let read_io_buffer_layout = Layout::from_size_align(args.block_size as usize, 1).unwrap();
            let read_io_buffer = unsafe { alloc(read_io_buffer_layout) };
            let read_buffer_mr = unsafe {
                rdma_binding::ibv_reg_mr(
                    pd,
                    read_io_buffer as *mut c_void,
                    args.block_size as size_t,
                    (rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
                        | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_READ
                        | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE)
                        as c_int,
                )
            };
            let read_io_buffer_rkey = unsafe { (*read_buffer_mr).rkey };
            not_ready_clients.fetch_sub(1, Ordering::SeqCst);
            println!("Client {} is ready.", i);
            while not_ready_clients.load(Ordering::SeqCst) > 0 {}
            println!("benchmark is starting...");
            let duration = Duration::from_secs(duration_seconds);
            let mut total = Duration::ZERO;
            let mut total_io = 0;
            let mut step = 0usize;

            while total < duration {
                let before = Instant::now();
                while quota > 0 {
                    quota -= 1;
                    let lba = lbas[step];
                    let is_write_mode = io_write_mode[step];
                    match is_write_mode {
                        true => {
                            debug_println_verbose!("post_remote_io_write");
                            per_io_time_tracker.push_back(Instant::now());
                            transport
                                .post_remote_io_write(step as u16, lba, write_io_buffer, args.block_size, write_io_buffer_rkey)
                                .expect("failed to post remote_io_write");
                        }
                        false => {
                            debug_println_verbose!("post_remote_io_read");
                            per_io_time_tracker.push_back(Instant::now());
                            transport
                                .post_remote_io_read(step as u16, lba, read_io_buffer, args.block_size, read_io_buffer_rkey)
                                .expect("failed to post remote_io_read");
                        }
                    }

                    step = (step + 1) % lbas.len();
                }

                let (ns, nf) = transport.poll_completions_reset().unwrap();
                debug_println_verbose!("completed I/O: {} success {} fail", ns, nf);
                total_io += (ns + nf) as usize;

                for _i in 0..(ns + nf) {
                    let latency = per_io_time_tracker.pop_front().unwrap().elapsed().as_nanos() as u64;
                    hist.record(latency.max(1)).unwrap() // avoid 0
                }
                quota = quota + (ns + nf) as usize;
                let elapsed = before.elapsed();
                total += elapsed;
                if nf > 0 {
                    eprintln!("Error I/O occurred!");
                    break;
                }
            }

            let bw = (total_io * block_size as usize) as f64 / total.as_secs_f64();
            println!("Client {} is done.", i);

            (hist, bw, total.as_secs_f64())
        });

        handles.push(handle);
    }

    let mut global_histogram: Histogram<u64> = Histogram::new_with_bounds(1u64, 300_000_000_000u64, 3).unwrap();
    let mut sum_bw = 0f64;
    let mut max_runtime = 0f64;

    for handle in handles {
        match handle.join() {
            Ok((local_histogram, local_bw, total_runtime)) => {
                sum_bw += local_bw;
                global_histogram.add(local_histogram).expect("TODO: Failed to combine histogram");
                max_runtime = max_runtime.max(total_runtime);
            }
            Err(e) => {
                println!("Thread panicked: {:?}", e);
            }
        }
    }

    println!("Benchmark is done. Calculating result...");

    let bandwidth = sum_bw / args.client.clone() as f64;
    let io_per_sec = global_histogram.len() as f64 / max_runtime;
    let latency_min = global_histogram.min(); // nanoseconds
    let latency_percentile_25 = global_histogram.value_at_quantile(0.25); // nanoseconds
    let latency_percentile_50 = global_histogram.value_at_quantile(0.5); // nanoseconds
    let latency_percentile_75 = global_histogram.value_at_quantile(0.75); // nanoseconds
    let latency_percentile_90 = global_histogram.value_at_quantile(0.9); // nanoseconds
    let latency_percentile_99 = global_histogram.value_at_quantile(0.99); // nanoseconds
    let latency_max = global_histogram.max(); // nanoseconds
    print_result(args.mode.clone(), args.workload.clone(), args.block_size, args.queue_depth.clone(),
                 bandwidth, io_per_sec, latency_min, latency_percentile_25,
                 latency_percentile_50, latency_percentile_75, latency_percentile_90,
                 latency_percentile_99, latency_max);
    println!("total I/O: {}", global_histogram.len());
}
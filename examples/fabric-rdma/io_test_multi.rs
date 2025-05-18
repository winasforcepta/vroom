use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use libc::size_t;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::alloc::{alloc, dealloc, Layout};
use std::collections::VecDeque;
use std::net::IpAddr;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{fmt, ptr, thread};
use std::sync::{Arc, Mutex};
#[cfg(any(debug_mode, debug_mode_verbose))]
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

    #[arg(long, default_value_t = false)] // 16GB default
    in_capsule_data: bool,
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

fn print_result(
    mode: IOMode,
    workload: Workload,
    block_size: u32,
    queue_depth: u32,
    bandwidth: f64,
    io_per_sec: f64,
    latency_percentile_010: u64,
    latency_percentile_050: u64,
    latency_percentile_100: u64,
    latency_percentile_200: u64,
    latency_percentile_300: u64,
    latency_percentile_500: u64,
    latency_percentile_600: u64,
    latency_percentile_700: u64,
    latency_percentile_800: u64,
    latency_percentile_900: u64,
    latency_percentile_950: u64,
    latency_percentile_990: u64,
    latency_percentile_995: u64,
    latency_percentile_999: u64,
) -> () {
    let mode_str: String = match mode {
        IOMode::Read => { "Read".to_string() },
        IOMode::Write => { "Write".to_string() }
        IOMode::Mixed => { "Mixed".to_string() }
    };
    let workload_str = match workload {
        Workload::Sequential => { "Sequential".to_string() }
        Workload::Random => { "Random".to_string() }
    };
    println!("\"mode\", \"workload\", \"block_size\", \"queue_depth\", \"bandwidth\", \"io_per_sec\", \"latency_percentile_010_us\", \"latency_percentile_050_us\", \"latency_percentile_100_us\", \"latency_percentile_200_us\", \"latency_percentile_300_us\", \"latency_percentile_500_us\", \"latency_percentile_600_us\", \"latency_percentile_700_us\", \"latency_percentile_800_us\", \"latency_percentile_900_us\", \"latency_percentile_950_us\", \"latency_percentile_990_us\", \"latency_percentile_995_us\", \"latency_percentile_999_us\"");
    println!("\"{}\", \"{}\", \"{}\", \"{}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\", \"{:.2}\"",
             mode_str,
             workload_str,
             block_size,
             queue_depth,
             bandwidth,
             io_per_sec,
             latency_percentile_010 as f64 / 1000f64,
             latency_percentile_050 as f64 / 1000f64,
             latency_percentile_100 as f64 / 1000f64,
             latency_percentile_200 as f64 / 1000f64,
             latency_percentile_300 as f64 / 1000f64,
             latency_percentile_500 as f64 / 1000f64,
             latency_percentile_600 as f64 / 1000f64,
             latency_percentile_700 as f64 / 1000f64,
             latency_percentile_800 as f64 / 1000f64,
             latency_percentile_900 as f64 / 1000f64,
             latency_percentile_950 as f64 / 1000f64,
             latency_percentile_990 as f64 / 1000f64,
             latency_percentile_995 as f64 / 1000f64,
             latency_percentile_999 as f64 / 1000f64
    );
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
    let not_ready_clients = Arc::from(AtomicUsize::new(args.client.clone() as usize));
    let connection_mtx = Arc::from(Mutex::new(true));

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
        let in_capsule_data = args.in_capsule_data.clone();

        let handle = thread::spawn(move || {
            let mut hist: Histogram<u64> = Histogram::new_with_bounds(1u64, 300_000_000_000u64, 3).unwrap();
            let bw;
            let mut total = Duration::ZERO;
            let total_size = args.queue_depth * args.block_size;
            let buffer_layout = Layout::from_size_align(total_size as usize, 4).unwrap();
            let io_buffer = unsafe { alloc(buffer_layout) };
            if io_buffer.is_null() {
                panic!("failed to allocate buffer");
            }
            let pattern = b"0123456789";
            let pattern_len = pattern.len();

            unsafe {
                let mut offset = 0;
                while offset + pattern_len <= total_size as usize {
                    ptr::copy_nonoverlapping(
                        pattern.as_ptr(),
                        io_buffer.add(offset),
                        pattern_len,
                    );
                    offset += pattern_len;
                }

                // Handle any remaining bytes if total_size is not a multiple of 10
                if offset < total_size as usize {
                    ptr::copy_nonoverlapping(
                        pattern.as_ptr(),
                        io_buffer.add(offset),
                        total_size as usize - offset,
                    );
                }

                if libc::mlock(io_buffer as *const _, total_size as size_t) != 0 {
                    panic!("failed to mlock");
                }
            }

            {
                let max_quota = quota;
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



                let buffer_mr = unsafe {
                    rdma_binding::ibv_reg_mr(
                        pd,
                        io_buffer as *mut c_void,
                        total_size as size_t,
                        (rdma_binding::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
                            | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_READ
                            | rdma_binding::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE)
                            as c_int,
                    )
                };
                let (buffer_rkey, buffer_lkey) = unsafe {
                    ((*buffer_mr).rkey, (*buffer_mr).lkey )
                };
                not_ready_clients.fetch_sub(1, Ordering::SeqCst);
                println!("Client {} is ready.", i);
                while not_ready_clients.load(Ordering::SeqCst) > 0 {}
                println!("benchmark is starting...");
                let duration = Duration::from_secs(duration_seconds);
                let mut total_io = 0;
                let mut step = 0usize;
                let mut cur_idx = 0usize;
                let before = Instant::now();

                while total < duration {
                    while quota > 0 {
                        quota -= 1;
                        let lba = lbas[step];
                        let is_write_mode = io_write_mode[step];
                        match is_write_mode {
                            true => unsafe {
                                #[cfg(any(debug_mode, debug_mode_verbose))]
                                debug_println_verbose!("post_remote_io_write");
                                if in_capsule_data {
                                    per_io_time_tracker.push_back(Instant::now());
                                    transport
                                        .post_remote_io_write_in_capsule_data(step as u16, lba, io_buffer.add(cur_idx * block_size as usize), block_size, buffer_rkey, buffer_lkey)
                                        .expect("failed to post remote_io_write");
                                } else {
                                    per_io_time_tracker.push_back(Instant::now());
                                    transport
                                        .post_remote_io_write(step as u16, lba, io_buffer.add(cur_idx * block_size as usize), block_size, buffer_rkey)
                                        .expect("failed to post remote_io_write");
                                }

                            }
                            false => unsafe {
                                #[cfg(any(debug_mode, debug_mode_verbose))]
                                debug_println_verbose!("post_remote_io_read");
                                if in_capsule_data {
                                    per_io_time_tracker.push_back(Instant::now());
                                    transport
                                        .post_remote_io_read_in_capsule_data(step as u16, lba, io_buffer.add(cur_idx * block_size as usize), block_size, buffer_rkey, buffer_lkey)
                                        .expect("failed to post remote_io_read");
                                } else {
                                    per_io_time_tracker.push_back(Instant::now());
                                    transport
                                        .post_remote_io_read(step as u16, lba, io_buffer.add(cur_idx * block_size as usize), block_size, buffer_rkey)
                                        .expect("failed to post remote_io_read");
                                }

                            }
                        }

                        step = (step + 1) % lbas.len();
                        cur_idx = (cur_idx + 1) % args.queue_depth as usize;
                    }

                    let (ns, nf) = transport.poll_completions_reset().unwrap();
                    #[cfg(any(debug_mode, debug_mode_verbose))]
                    debug_println_verbose!("completed I/O: {} success {} fail", ns, nf);
                    total_io += (ns + nf) as usize;

                    for _i in 0..(ns + nf) {
                        let latency = per_io_time_tracker.pop_front().unwrap().elapsed().as_nanos() as u64;
                        hist.record(latency.max(1)).unwrap() // avoid 0
                    }
                    quota = quota + (ns + nf) as usize;
                    total = before.elapsed();
                    if nf > 0 {
                        eprintln!("Error I/O occurred!");
                        break;
                    }
                }

                // let mut retry: i64 = 1 << 20;
                println!("Time out with {} inflight I/O after completing {} I/O. Draining...", max_quota - quota, total_io.clone());
                let mut retry = 1 << 13;
                while quota < max_quota && retry > 0 {
                    let (ns, nf) = transport.poll_completions_reset().unwrap();
                    quota = quota + (ns + nf) as usize;
                    retry -= 1;
                }

                // while quota < max_quota {
                //     if retry == 0 {
                //         println!("{} remaining. Draining...", max_quota - quota);
                //         retry = 1 << 15;
                //     }
                //     let (ns, nf) = transport.poll_completions_reset().unwrap();
                //     #[cfg(any(debug_mode, debug_mode_verbose))]
                //     debug_println_verbose!("completed I/O: {} success {} fail", ns, nf);
                //     total_io += (ns + nf) as usize;
                //
                //     for _i in 0..(ns + nf) {
                //         let latency = per_io_time_tracker.pop_front().unwrap().elapsed().as_nanos() as u64;
                //         hist.record(latency.max(1)).unwrap() // avoid 0
                //     }
                //     quota = quota + (ns + nf) as usize;
                //     total = before.elapsed();
                //     if ns + nf == 0 {
                //         retry = retry - 1;
                //     }
                // }
                bw = (total_io * block_size as usize) as f64 / total.as_secs_f64();
                println!("Client {} is done.", i);
            }

            thread::sleep(Duration::from_millis(5000));

            unsafe {
                let _ = libc::munlock(io_buffer as *const _, total_size as size_t);
                dealloc(io_buffer, buffer_layout);
            }


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
    // latencies in nanoseconds
    let latency_min = global_histogram.min();
    let latency_percentile_010 = global_histogram.value_at_quantile(0.010);
    let latency_percentile_050 = global_histogram.value_at_quantile(0.050);
    let latency_percentile_100 = global_histogram.value_at_quantile(0.100);
    let latency_percentile_200 = global_histogram.value_at_quantile(0.200);
    let latency_percentile_300 = global_histogram.value_at_quantile(0.300);
    let latency_percentile_500 = global_histogram.value_at_quantile(0.500);
    let latency_percentile_600 = global_histogram.value_at_quantile(0.600);
    let latency_percentile_700 = global_histogram.value_at_quantile(0.700);
    let latency_percentile_800 = global_histogram.value_at_quantile(0.800);
    let latency_percentile_900 = global_histogram.value_at_quantile(0.900);
    let latency_percentile_950 = global_histogram.value_at_quantile(0.950);
    let latency_percentile_990 = global_histogram.value_at_quantile(0.990);
    let latency_percentile_995 = global_histogram.value_at_quantile(0.995);
    let latency_percentile_999 = global_histogram.value_at_quantile(0.999);
    print_result(
        args.mode.clone(),
        args.workload.clone(),
        args.block_size,
        args.queue_depth.clone(),
        bandwidth,
        io_per_sec,
        latency_percentile_010,
        latency_percentile_050,
        latency_percentile_100,
        latency_percentile_200,
        latency_percentile_300,
        latency_percentile_500,
        latency_percentile_600,
        latency_percentile_700,
        latency_percentile_800,
        latency_percentile_900,
        latency_percentile_950,
        latency_percentile_990,
        latency_percentile_995,
        latency_percentile_999
    );
    println!("total I/O: {}. Total time: {}", global_histogram.len(), max_runtime);
}
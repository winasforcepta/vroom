use rand::{thread_rng, Rng};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, process, thread};
use vroom::memory::*;
use vroom::{NvmeDevice, QUEUE_LENGTH, HUGE_PAGE_SIZE};

pub fn main() -> Result<(), Box<dyn Error>> {
    // let mut args = env::args();
    // args.next();
    //
    // let pci_addr = match args.next() {
    //     Some(arg) => arg,
    //     None => {
    //         eprintln!("Usage: cargo run --example io_test_sgl <pci bus id> [duration in seconds]");
    //         process::exit(1);
    //     }
    // };
    //
    // let duration = match args.next() {
    //     Some(secs) => Some(Duration::from_secs(
    //         secs.parse().expect("Duration in seconds should be an integer"),
    //     )),
    //     None => None,
    // };
    //
    // let nvme = vroom::init(&pci_addr)?;
    //
    // // Run two tests with different batch sizes.
    // let nvme = qd_n(nvme, 1, 0, false, 128, duration)?;
    // let _ = qd_n(nvme, 1, 0, false, 256, duration)?;

    Ok(())
}
//
// /// Multi-threaded test function using SGL-based submissions.
// /// Each thread preallocates an SGL table (one DMA buffer for 128 descriptors)
// /// and reuses it for all submissions.
// fn qd_n(
//     nvme: NvmeDevice,
//     n_threads: u64,
//     _n: u64,
//     write: bool,
//     batch_size: usize,
//     time: Option<Duration>,
// ) -> Result<NvmeDevice, Box<dyn Error>> {
//     let blocks: u64 = 8;
//     let ns_blocks = nvme.namespaces.get(&1).unwrap().blocks / blocks;
//
//     let nvme = Arc::new(Mutex::new(nvme));
//     let mut threads = Vec::new();
//
//     for _ in 0..n_threads {
//         let nvme = Arc::clone(&nvme);
//         let range = (0, ns_blocks);
//
//         let handle = thread::spawn(move || -> (u64, f64) {
//             let mut rng = thread_rng();
//             // Number of bytes per I/O submission.
//             let bytes: usize = 512 * blocks as usize;
//             let mut total = Duration::ZERO;
//             // Allocate a per-thread DMA buffer for I/O data.
//             let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
//             // Fill the buffer with random data (for write operations).
//             let rand_block: Vec<u8> = (0..(32 * bytes)).map(|_| rng.gen()).collect();
//             buffer[0..32 * bytes].copy_from_slice(&rand_block);
//
//             // Create an I/O queue pair.
//             let mut qpair = nvme
//                 .lock()
//                 .unwrap()
//                 .create_io_queue_pair(QUEUE_LENGTH)
//                 .unwrap();
//
//             // Preallocate an SGL table once per thread.
//             // This table holds 128 SGL descriptors.
//             let mut sgl_table: Dma<[NvmeSglDescriptor; 128]> =
//                 Dma::allocate(128 * std::mem::size_of::<NvmeSglDescriptor>())
//                     .unwrap();
//
//             // Declare a counter with explicit type (i32).
//             let mut ctr: i32 = 0;
//             if let Some(duration) = time {
//                 let mut ios: u64 = 0;
//                 while total < duration {
//                     let lba = rng.gen_range(range.0..range.1);
//                     let before = Instant::now();
//                     // Poll completions.
//                     while let Some(_) = qpair.quick_poll() {
//                         ctr = ctr.saturating_sub(1);
//                         ios += 1;
//                     }
//                     if ctr == batch_size as i32 {
//                         qpair.complete_io(1);
//                         ctr = ctr.saturating_sub(1);
//                         ios += 1;
//                     }
//                     // Compute slice indices from the buffer.
//                     let start = (ctr as usize).saturating_mul(bytes);
//                     let end = start + bytes;
//                     // Submit I/O using the SGL path.
//                     qpair.submit_io_sgl(
//                         &buffer.slice(start..end),
//                         lba * blocks,
//                         write,
//                         &mut sgl_table,
//                     );
//                     total += before.elapsed();
//                     ctr += 1;
//                 }
//                 if ctr != 0 {
//                     let before = Instant::now();
//                     qpair.complete_io(ctr as usize);
//                     total += before.elapsed();
//                 }
//                 ios += ctr as u64;
//                 assert!(qpair.sub_queue.is_empty());
//                 nvme.lock().unwrap().delete_io_queue_pair(qpair).unwrap();
//
//                 (ios, ios as f64 / total.as_secs_f64())
//             } else {
//                 let seq: Vec<u64> = (0..ns_blocks)
//                     .map(|_| rng.gen_range(range.0..range.1))
//                     .collect();
//                 for &lba in seq.iter() {
//                     let before = Instant::now();
//                     while let Some(_) = qpair.quick_poll() {
//                         ctr = ctr.saturating_sub(1);
//                     }
//                     if ctr == 32 {
//                         qpair.complete_io(1);
//                         ctr = ctr.saturating_sub(1);
//                     }
//                     let start = (ctr as usize).saturating_mul(bytes);
//                     let end = start + bytes;
//                     qpair.submit_io_sgl(&buffer.slice(start..end), lba * blocks, write, &mut sgl_table);
//                     total += before.elapsed();
//                     ctr += 1;
//                 }
//                 if ctr != 0 {
//                     let before = Instant::now();
//                     qpair.complete_io(ctr as usize);
//                     total += before.elapsed();
//                 }
//                 assert!(qpair.sub_queue.is_empty());
//                 nvme.lock().unwrap().delete_io_queue_pair(qpair).unwrap();
//                 (ns_blocks, ns_blocks as f64 / total.as_secs_f64())
//             }
//         });
//         threads.push(handle);
//     }
//
//     let total = threads.into_iter().fold((0, 0.0), |acc, thread| {
//         let res = thread.join().expect("Thread execution failed!");
//         (acc.0 + res.0, acc.1 + res.1)
//     });
//     println!(
//         "n: {}, total {} iops: {:?}",
//         total.0,
//         if write { "write" } else { "read" },
//         total.1
//     );
//
//     match Arc::try_unwrap(nvme) {
//         Ok(mutex) => match mutex.into_inner() {
//             Ok(t) => Ok(t),
//             Err(e) => Err(e.into()),
//         },
//         Err(_) => Err("Arc::try_unwrap failed, not the last reference.".into()),
//     }
// }
//
// /// Optionally fill the namespace using SGL-based writes.
// #[allow(unused)]
// fn fill_ns(nvme: &mut NvmeDevice) {
//     let buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
//     let max_lba = nvme.namespaces.get(&1).unwrap().blocks - (buffer.size as u64 / 512) - 1;
//     let blocks = buffer.size as u64 / 512;
//     let mut lba = 0;
//     while lba < max_lba - 512 {
//         nvme.write_sgl(&buffer, lba).unwrap();
//         lba += blocks;
//     }
// }

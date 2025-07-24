use std::error::Error;
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{env, process};

use vroom::HUGE_PAGE_SIZE;

use crate::util::{combine_results, IoLog};

mod util;
mod features;
mod benchmarks;

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: ./nvmebench <pci bus id>");
            process::exit(1);
        }
    };

    let mut nvme = vroom::init(&pci_addr)?;
    let mut result = Vec::new();


    //features::print_identify_controller_info(&nvme.identify_controller_info);

    let max_io_size_per_thread = 1024*1024*1024 * 64;

    for write in [true] {
        for io_size_per_request in [8192, 1024*1024] {
            for queue_depth in [1,32,128] {
                for num_threads in [1, 8, 32] {
                    (nvme, result) = benchmarks::determine_cache_size(nvme, max_io_size_per_thread/num_threads, io_size_per_request, write, queue_depth, num_threads as usize);
                    println!(
                    "max_io_size_per_thread: {:?}, io_size_per_request: {:?}, write: {:?}, queue_depth: {:?}, num_threads: {:?}",
                     max_io_size_per_thread / num_threads, io_size_per_request, write, queue_depth, num_threads
                    );
                    eprintln!(
                    "max_io_size_per_thread: {:?}, io_size_per_request: {:?}, write: {:?}, queue_depth: {:?}, num_threads: {:?}",
                     max_io_size_per_thread / num_threads, io_size_per_request, write, queue_depth, num_threads
                    );
                    println!("Result vec: {:?}", combine_results(&result, Duration::from_secs(1)));
                    println!("\n\n\n\n\n");
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }

    
    

    /*
    nvme = benchmarks::full_random_combinations(nvme);
    println!("");
    nvme = benchmarks::single_lba(nvme, true);
    nvme = benchmarks::single_lba(nvme, false);
    println!("");
    nvme = benchmarks::zipf_single_action(nvme, true);
    nvme = benchmarks::zipf_single_action(nvme, false); */


    Ok(())
}



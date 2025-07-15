use std::error::Error;
use std::{env, process};

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


    features::print_identify_controller_info(&nvme.identify_controller_info);

    nvme = benchmarks::determine_cache_size(nvme, 1024*1024*1024 / 8);
    nvme = benchmarks::full_random_combinations(nvme);
    println!("");
    nvme = benchmarks::single_lba(nvme, true);
    nvme = benchmarks::single_lba(nvme, false);
    println!("");
    nvme = benchmarks::zipf_single_action(nvme, true);
    nvme = benchmarks::zipf_single_action(nvme, false);


    Ok(())
}
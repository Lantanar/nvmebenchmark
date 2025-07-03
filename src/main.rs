use std::error::Error;
use std::{env, process};

mod features;
mod util;

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example hello_world <pci bus id>");
            process::exit(1);
        }
    };

    let mut nvme = vroom::init(&pci_addr)?;


    features::print_identify_controller_info(&nvme.identify_controller_info);

    nvme = util::determine_cache_size(nvme);
    nvme = util::simple_test(nvme);

    Ok(())
}
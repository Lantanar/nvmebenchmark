use core::num;
use std::{cmp::min, time::Instant};
use rand::{rngs::SmallRng, seq::SliceRandom, RngCore, SeedableRng};
use std::error::Error;
use vroom::{memory::{Dma, DmaSlice}, NvmeQueuePair};
use rand_distr::{num_traits, Distribution};


pub const ONE_GIB: u64 = 1024 * 1024 * 1024;

pub struct QueuePairError {
    pub(crate) queue_pair: NvmeQueuePair,
    message: String,
}

impl std::fmt::Debug for QueuePairError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "submit_io error: {}", self.message)
    }
}

impl Error for QueuePairError {}
impl std::fmt::Display for QueuePairError { 
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "submit_io error: {}", self.message)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Allocation {
    pub lba: u64,
    pub start: usize,
    pub stop: usize,
}

pub fn threadsafe_io_batch_complete_64(mut queue_pair: NvmeQueuePair, ns_id: u32, block_size: u64, data: (&Dma<u8>, &Vec<Allocation >), write: bool) -> Result<NvmeQueuePair, Box<QueuePairError>> {
    let batch_size = 64;
    
    let mut total = 0;
    for alloc in data.1 {
        if alloc.stop <= alloc.start {
            continue;
        }
        let res = queue_pair.submit_io(ns_id, block_size, &data.0.slice(alloc.start..alloc.stop), alloc.lba, write);
        if res == 0 {
            if total > 0 {
                queue_pair.complete_io(total);
            }
            return Err(Box::new(QueuePairError{queue_pair, message: "Request was not queued".into()}));
        }

        total += res;
    
        if total > batch_size {
            //complete but don't let the submission queue run out of entries
            queue_pair.complete_io(total/2);
            total -= total / 2;
        }
    }

    if total > 0 {
        queue_pair.complete_io(total);
    }
    Ok(queue_pair)
}

pub fn create_random_data(size: usize) -> Dma<u8> {
    let mut rng = SmallRng::seed_from_u64(1);
    let mut data: Dma<u8> = Dma::allocate(size).unwrap();
    for i in 0..size / 8 {
        data[i * 8..(i + 1) * 8].copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    data
}

pub fn construct_allocation_from_distribution<D, T>(total_size: usize, ram_size: usize, block_size: u64, distribution: D) -> Vec<Allocation>
where D:Distribution<T>, T: rand_distr::num_traits::NumCast  {
    let mut allocations = Vec::new();

    let mut rng = SmallRng::from_rng(&mut rand::rng());
    let mut distr = distribution.sample_iter(&mut rng);

    for i in 0..total_size / block_size as usize {
        let lba = (&mut distr).next().unwrap();
        let start = (i * block_size as usize) % ram_size;
        let stop = start + block_size as usize;
        allocations.push(Allocation { lba: num_traits::cast(lba).unwrap(), start, stop });
    }

    return allocations;
}

pub fn construct_random_allocations(size: usize, max_block_amount: u64, block_size: u64, random_from: bool, random_to: bool) -> Vec<Allocation> {    
    let mut size = size;
    let mut num_blocks = size as u64 / block_size;
    let mut rng = SmallRng::seed_from_u64(1);
    let mut lbas = Vec::with_capacity(num_blocks as usize);

    let start = match get_random_safe_start(size as u64, max_block_amount, block_size) {
        Some(start) => start,
        None => {
            size = (max_block_amount * block_size) as usize;
            num_blocks = max_block_amount;
            0
        }
    };

    for i in start..start+num_blocks {
        lbas.push(i as u64);
    }
    if random_to {
        lbas.shuffle(&mut rng);
    }

    let mut slices = Vec::with_capacity(num_blocks as usize);
    for i in 0..num_blocks {
        slices.push(((i*block_size) as usize, ((i+1)*block_size) as usize));
    }
    if random_from {
        slices.shuffle(&mut rng);
    }

    return lbas.into_iter().zip(slices).map(|(lba, (start, stop))| Allocation { lba, start, stop }).collect();
}

pub fn get_random_safe_start(op_size: u64, max_blocks: u64, block_size: u64) -> Option<u64> {
    if op_size / block_size > max_blocks  {
        return None;
    }

    return Some(rand::rng().next_u64() % (max_blocks - op_size / block_size));
}

pub fn print_2x2(results: &[u128]) {
    println!("\n--- Results (MiB/s)---");
    println!("{}| {} | {}", "              ", "randto=false", "randto=true ");
    println!("--------------------------");
    println!("{}| {:<12} | {:<12}", "randfrom=false", results[0], results[1]);
    println!("{}| {:<12} | {:<12}", "randfrom=true ",  results[2], results[3]);
}

const QUEUE_LENGTH: usize = 1024;
use core::num;
use std::{cmp::min, io, result, time::{Duration, Instant}};
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

#[derive(Debug, Clone)]
pub struct IoLog {
    pub start: Instant,
    pub end: Instant,
    pub actions: usize,
    pub cumulative_size: usize,
}

#[derive(Copy, Clone, Debug)]
pub struct Allocation {
    pub lba: u64,
    pub start: usize,
    pub stop: usize,
}

/**
 * @returns a vector of chronological sorted (actions, cumulative_size) tuples, each representing a bucket of the given duration
 */
pub fn combine_results(results: &Vec<Vec<IoLog>>, bucket_duration: Duration) -> Vec<(f64, f64)> {

    let results : Vec<_> = results.iter().filter(|v| !v.is_empty()).collect();
    if results.is_empty() {
        return Vec::new();
    }

    let min_start = results.iter().map(|x| x.into_iter().min_by_key(|log| log.start).unwrap()).min_by_key(|log| log.start).unwrap().start;
    let max_end = results.iter().map(|x| x.into_iter().max_by_key(|log| log.end).unwrap()).max_by_key(|log| log.end).unwrap().end;
    let total_duration = max_end.duration_since(min_start);
    let num_buckets = (total_duration.as_micros() / bucket_duration.as_micros() + 1) as usize;

    let mut results_combined = vec![(0.0,0.0); num_buckets];

    for io_log in results.iter().flat_map(|x| x.iter()) {
        let log_duration = io_log.end.duration_since(io_log.start);
        if log_duration.is_zero() {
            continue;
        }

        // normalize all duration to the first logged start time
        let start_offset = io_log.start - min_start;
        let end_offset = io_log.end - min_start;

        let mut current_time = start_offset;

        while current_time < end_offset {
            let bucket_index = (current_time.as_secs_f64() / bucket_duration.as_secs_f64()) as usize;
            if bucket_index >= num_buckets { break; }

            let bucket_end_time = (bucket_index + 1) as f64 * bucket_duration.as_secs_f64();
            let overlap_end = end_offset.as_secs_f64().min(bucket_end_time);

            let overlap_share = (overlap_end - current_time.as_secs_f64()) / log_duration.as_secs_f64();

            results_combined[bucket_index].0 += overlap_share * io_log.actions as f64;
            results_combined[bucket_index].1 += overlap_share * io_log.cumulative_size as f64;

            current_time = Duration::from_secs_f64(overlap_end);
        }
    }

    results_combined
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
use std::{cmp::min, f32::MIN, sync::Arc, time::Duration};
use rand::{rngs::SmallRng, seq::SliceRandom, Rng, RngCore, SeedableRng};
use std::sync::Mutex;
use std::time::Instant;
use std::error::Error;
use std::thread;
use vroom::{memory::{Dma, DmaSlice}, NvmeDevice, NvmeQueuePair, HUGE_PAGE_SIZE};


const MIN_DATA_SIZE: u64 = 4096;
const HUGE_PAGE_BITS: u32 = 21;

struct QueuePairError {
    queue_pair: NvmeQueuePair,
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

fn test_write_singlethread_batch_complete_64(mut queue_pair: NvmeQueuePair, ns_id: u32, block_size: u64, data: (&Dma<u8>, &Vec<(u64, (usize, usize))>)) -> Result<NvmeQueuePair, Box<QueuePairError>> {
    let batch_size = 64;
    
    let mut total = 0;
    for (lba, (start, stop)) in data.1 {
        if stop <= start {
            continue;
        }
        let res = queue_pair.submit_io(ns_id, block_size, &data.0.slice(*start..*stop), *lba, true);
        if res == 0 {
            if total > 0 {
                queue_pair.complete_io(total);
            }
            return Err(Box::new(QueuePairError{queue_pair, message: "Request was not queued".into()}));
        }

        total += res;
    
        if total > batch_size {
            while queue_pair.quick_poll().is_some() {
                total -= 1;
            }
        }
    }

    if total > 0 {
        queue_pair.complete_io(total);
    }
    Ok(queue_pair)
}

fn create_random_data(size: usize) -> Dma<u8> {
    let mut rng = SmallRng::seed_from_u64(1);
    let mut data: Dma<u8> = Dma::allocate(size).unwrap();
    for i in 0..size / 8 {
        data[i * 8..(i + 1) * 8].copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    data
}

fn construct_random_allocations(size: usize, mut max_block_amount: u64, block_size: u64, random_from: bool, random_to: bool) -> Vec<(u64, (usize, usize))> {
    max_block_amount -= MIN_DATA_SIZE as u64 / block_size -1; 
    
    let num_blocks = min (size / MIN_DATA_SIZE as usize, max_block_amount as usize);

    let mut lbas = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        lbas.push(i as u64);
    }
    if random_to{
        lbas.shuffle(&mut SmallRng::seed_from_u64(1));
    }

    let mut slices = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        slices.push((i*MIN_DATA_SIZE as usize, (i+1)*MIN_DATA_SIZE as usize));
    }
    if random_from {
        slices.shuffle(&mut SmallRng::seed_from_u64(1));
    }

    return lbas.into_iter().zip(slices).collect();
}

fn test_single_write(queue_pair: NvmeQueuePair, ns_id: u32, block_size: u64, data:&Dma<u8>) -> Result<NvmeQueuePair, Box<QueuePairError>> {
    let mut allocations = Vec::new();
    allocations.push((1, (0, data.size)));
    let data = (data, &allocations);

    return test_write_singlethread_batch_complete_64(queue_pair, ns_id, block_size, data);
}

pub fn simple_test(mut nvme: NvmeDevice) -> NvmeDevice {
    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let mut dma = create_random_data(HUGE_PAGE_SIZE);

    

    const NUM_IT: usize = 10;
    let mut total= 0;

    for _ in 0..NUM_IT {
        let mut queue_pair = nvme.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        let t = std::time::Instant::now();
        queue_pair = test_single_write(queue_pair, ns_id, block_size, &dma).unwrap();
        let duration = t.elapsed();
        total += duration.as_micros();
        nvme.delete_io_queue_pair(queue_pair);
    }
    println!("Continous write of {:?} Bytes of data completed with average: {:?} MiB/s", HUGE_PAGE_SIZE, (15625 * HUGE_PAGE_SIZE as u128 * NUM_IT as u128) / (16384 * total as u128));


    
    dma = create_random_data(HUGE_PAGE_SIZE * 10);
    let mut results = Vec::new();

    for random_from in [false, true] {
        for random_to in [false, true] {
            let allocations = construct_random_allocations(dma.size, max_blocks, block_size, random_from, random_to);
            total = 0;
            for _ in 0..NUM_IT {
                let mut queue_pair = nvme.create_io_queue_pair(min(allocations.len(), 1024)).unwrap();
                let t = std::time::Instant::now();
                queue_pair = match test_write_singlethread_batch_complete_64(queue_pair, ns_id, block_size, (&dma, &allocations)) {
                    Ok(qp) => qp,
                    Err(e) => {
                        //TODO
                        e.queue_pair
                    }
                };
                let duration = t.elapsed();
                nvme.delete_io_queue_pair(queue_pair);
                total += duration.as_micros();
            }
            results.push((15625 * 10 * HUGE_PAGE_SIZE as u128 * NUM_IT as u128) / (16384 * total as u128));
        }
    }

    print_2x2(&results);

    nvme
}

pub fn determine_cache_size(mut nvme: NvmeDevice) -> NvmeDevice {
    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let dma = create_random_data(HUGE_PAGE_SIZE);

    let mut results = Vec::new();

    let mut it_check = 0;
    let mut current_block = 1;
    let mut t = 0;
    let mut last_t = 0;

    let mut queue_pair = nvme.create_io_queue_pair(QUEUE_LENGTH).unwrap();

    while ((t < last_t * 4 / 3 || it_check < 10) && it_check < 1000) {
        let v = vec![(current_block, (0, dma.size))];
        let start = Instant::now();
        queue_pair = match test_write_singlethread_batch_complete_64(queue_pair, ns_id, block_size, (&dma, &v)) {
            Ok(qp) => qp,
            Err(e) => {
                eprintln!("Error during write: {}", e.message);
                e.queue_pair
                }
            };
        let duration = start.elapsed();
        last_t = t;
        t = duration.as_micros();
        results.push(t);
        it_check += 1;
        current_block += (HUGE_PAGE_SIZE as u64 / block_size) % max_blocks;
    }

    for (i, &result) in results.iter().enumerate() {
        println!("Iteration {}: {} us", i + 1, result);
    }

    nvme.delete_io_queue_pair(queue_pair);

    nvme
}

fn print_2x2(results: &[u128]) {
    println!("\n--- Results Grid ---");
    println!("{}| {} | {}", "              ", "randto=false", "randto=true ");
    println!("--------------------------");
    println!("{}| {:<12} | {:<12}", "randfrom=false", results[0], results[1]);
    println!("{}| {:<12} | {:<12}", "randfrom=true ",  results[2], results[3]);
}

const QUEUE_LENGTH: usize = 1024;
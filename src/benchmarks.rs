use crate::util::{construct_allocation_from_distribution, construct_random_allocations, create_random_data, get_random_safe_start, print_2x2, threadsafe_io_batch_complete_64, QueuePairError, ONE_GIB};
use rand_distr::Zipf;
use vroom::{NvmeQueuePair, memory::{Dma, DmaSlice}, NvmeDevice, HUGE_PAGE_SIZE};  
use rand::{rngs::SmallRng, Rng, RngCore, SeedableRng};
use std::{cmp::min, time::Instant};


pub fn determine_cache_size(mut nvme: NvmeDevice, max_write: i64) -> NvmeDevice {
    let mut max_write = if max_write <= 0 {
        ONE_GIB * 8
    } else {
        max_write as u64
    };

    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let dma = create_random_data(HUGE_PAGE_SIZE);

    if max_write >= max_blocks * block_size {
        max_write = max_blocks * block_size / 2;
    }

    let mut results = Vec::new();

    let mut rng = SmallRng::seed_from_u64(Instant::now().elapsed().as_millis() as u64);

    let batch_size = 64;

    let step_size = HUGE_PAGE_SIZE as u64 / block_size;
    let mut it_check = 0;
    let mut total = 0;
    let mut cumulative_actions: usize = 0;
    let mut t = 0;
    let mut last_t = 0;
    let start_block = rng.next_u64() % (max_blocks - max_write / block_size);
    let mut queue_pair = nvme.create_io_queue_pair(batch_size * 2).unwrap();
    let mut start = Instant::now();

    while (t < last_t * 4 / 3 && it_check < max_write / block_size) || it_check < step_size*8 {

        let res = queue_pair.submit_io(ns_id, block_size, &dma.slice(0..block_size as usize), start_block+it_check, true);
        if res == 0 {
            println!("Request was not queued, results will be inaccurate");
        }

        total += res;
    
        if total > batch_size {
            //complete, but don't let the submission queue run out of entries
            queue_pair.complete_io(total/2);
            cumulative_actions += total / 2;
            total -= total / 2;

            if cumulative_actions > step_size as usize {
                let duration = start.elapsed();
                last_t = t;
                t = duration.as_micros();
                results.push((cumulative_actions, t));
                cumulative_actions = 0;
                start = Instant::now();
            }
        }
        
        it_check += 1;
    }
    if total > 0 {
        queue_pair.complete_io(total);
        let duration = start.elapsed();
        t = duration.as_micros();
        results.push((total + cumulative_actions, t));
    }


    nvme.delete_io_queue_pair(queue_pair);

    if it_check >= step_size*8 {
        println!("Could not detect cache size, tested: {}MiB", it_check * block_size / (1024 * 1024));
    } else {
        println!("Cache size: {} MiB", it_check * block_size / (1024 * 1024));
    }

    let data_points = min(50, results.len());
    let mut condensed = Vec::with_capacity(data_points);

    for i in 0..data_points {
        let start = i * results.len() / data_points;
        let end = (i + 1) * results.len() / data_points;

        let chunk = &results[start..end];
        let chunk_len = chunk.len();

        if chunk_len > 0 {
            let n: usize = chunk.iter().map(|(actions, _)| actions).sum();
            let t: u128 = chunk.iter().map(|(_, time)| time).sum();
            condensed.push((n as u128 * block_size as u128 * 1_000_000) / (t * 1024 * 1024));
        }
    }

    condensed.iter().for_each(|x| println!("{} MiB/s", x));

    nvme
}

pub fn single_lba(mut nvme: NvmeDevice, write: bool) -> NvmeDevice {
    let n_loops = 32;

    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let dma = create_random_data(HUGE_PAGE_SIZE);

    let mut queue_pair = nvme.create_io_queue_pair(128).unwrap();

    
    let lba = get_random_safe_start(block_size, max_blocks, block_size).unwrap();

    if !write {
        let res = queue_pair.submit_io(ns_id, block_size, &dma.slice(0..block_size as usize), lba, true);
        queue_pair.complete_io(res);
    }

    let mut submitted = 0;
    let start = Instant::now();
    for _ in 0..n_loops * HUGE_PAGE_SIZE as u64 / block_size {
        submitted += queue_pair.submit_io(ns_id, block_size, &dma.slice(0..block_size as usize), lba, write);
        
        if submitted >= 64 {
            queue_pair.complete_io(submitted/2);
            submitted -= submitted / 2;
        }
        
    }
    if submitted > 0 {
        queue_pair.complete_io(submitted);
    }
    let duration = start.elapsed();
    println!("Continous {} same LBA completed with {}MiB/s", if write { "write to" } else { "read from" }, n_loops as u128 * HUGE_PAGE_SIZE as u128 * 1_000_000 / (duration.as_micros() * 1024 * 1024));

    nvme.delete_io_queue_pair(queue_pair);
    
    nvme
}

pub fn full_random_combinations(mut nvme: NvmeDevice) -> NvmeDevice {
    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let mut dma = create_random_data(HUGE_PAGE_SIZE);

    const NUM_IT: usize = 10;
    let mut total= 0;
    let mut results = Vec::new();

    let mut successfull_it = NUM_IT;

    let mut queue_pair = nvme.create_io_queue_pair(128).unwrap();

    for random_from in [false, true] {
        for random_to in [false, true] {
            let allocations = construct_random_allocations(dma.size, max_blocks, block_size, random_from, random_to);
            total = 0;
            for _ in 0..NUM_IT {
                
                let t = std::time::Instant::now();
                queue_pair = match threadsafe_io_batch_complete_64(queue_pair, ns_id, block_size, (&dma, &allocations), true) {
                    Ok(qp) => qp,
                    Err(e) => {
                        successfull_it -= 1;
                        e.queue_pair
                    }
                };
                let duration = t.elapsed();
                
                total += duration.as_micros();
            }
            results.push((1_000_000 * HUGE_PAGE_SIZE as u128 * successfull_it as u128) / (1024*1024 * total as u128));
        }
    }
    nvme.delete_io_queue_pair(queue_pair);

    print_2x2(&results);

    nvme
}

pub fn zipf_single_action(mut nvme: NvmeDevice, write: bool) -> NvmeDevice {
    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let dma = create_random_data(HUGE_PAGE_SIZE);

    let mut queue_pair = nvme.create_io_queue_pair(128).unwrap();

    for s in 1..3 {
        for n in [4096, 32768, 262144, 2097152] {
            let start_lba = match get_random_safe_start(n * block_size, max_blocks, block_size) {
                Some(x) => x,
                None => continue,
            };

            let distr = Zipf::new(n as f64, s as f64).unwrap();

            let mut allocations = construct_allocation_from_distribution(n as usize * block_size as usize, dma.size, block_size, distr);
            allocations = allocations.iter_mut().map(|x| {x.lba += start_lba; *x}).collect();

            if s==1 && n==4096 {
                println!("{:?}", allocations);
            }

            let t = std::time::Instant::now();
            queue_pair = match threadsafe_io_batch_complete_64(queue_pair, ns_id, block_size, (&dma, &allocations), write) {
                Ok(qp) => qp,
                Err(e) => {
                    eprintln!("Failed to complete some transactions, report may be innacurate");
                    e.queue_pair
                }
            };
            let d = t.elapsed();

            

            if d.as_micros() == 0 {
                println!("Unexpected error where elapsed time is 0");
                continue
            }

            println!("Zipf({},{}): {}MiB/s",n,s,n as u128 * block_size as u128 * 1_000_000/(d.as_micros() * 1024* 1024));
        }
    }
    nvme.delete_io_queue_pair(queue_pair);
    
    nvme
}
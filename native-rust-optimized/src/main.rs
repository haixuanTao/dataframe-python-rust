mod utils;

use rayon::prelude::*;
use utils::{NativeDataFrameRaw, NativeDataFrame};
use std::{collections::HashMap, fs::File, time::Instant};

fn use_native_rust(path: &str, path_wikipedia: &str, output_path: &str, ) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let file = File::open(path_wikipedia)?;
    let mut rdr_wiki = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let hash_wikipedia: HashMap<String, utils::WikiDataFrame> = rdr_wiki.deserialize()
        .into_iter()
        .filter_map(|result: csv::Result<utils::WikiDataFrame>| result.map(|x| (x.language.clone(), x)).ok())
        .collect();

    let file = File::open(path)?;
    let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let groups_hash = rdr.deserialize()
        .into_iter()
        .fold(HashMap::<String, (utils::GroupBy, usize)>::new(), |mut hash_group, record: csv::Result<utils::NativeDataFrame>| {
            let record = record.unwrap();
            let (group, count) = hash_group.entry(record.open_status.clone()).or_default();
            group.status = record.open_status;
            group.reputation_at_post_creation += record.reputation_at_post_creation;
            group.owner_undeleted_answer_count_at_post_time += record.owner_undeleted_answer_count_at_post_time;

            if let Some(wiki) = hash_wikipedia.get(&record.tag1) {
                group.imperative += wiki.imperative;
                group.object_oriented += wiki.object_oriented;
                group.functional += wiki.functional;
                group.procedural += wiki.procedural;
                group.generic += wiki.generic;
                group.reflective += wiki.reflective;
                group.event_driven += wiki.event_driven;
            }

            *count += 1;

            hash_group
        });

    let groups = groups_hash
        .into_iter()
        .map(|(_, (mut group, count))| {
            group.reputation_at_post_creation /= count as f64;
            group.owner_undeleted_answer_count_at_post_time /= count as f64;
            group.imperative /= count as f64;
            group.object_oriented /= count as f64;
            group.functional /= count as f64;
            group.procedural /= count as f64;
            group.generic /= count as f64;
            group.reflective /= count as f64;
            group.event_driven /= count as f64;

            group
        });

    let mut wtr = csv::Writer::from_path(output_path)?;
    for record in groups {
        wtr.serialize(record)?;
    }

    println!("Overall time taken: {:?}", start.elapsed());

    Ok(())
}

fn use_native_parallel_rust(path: &str, path_wikipedia: &str, output_path: &str, ) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let file = File::open(path_wikipedia)?;
    let mut rdr_wiki = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let hash_wikipedia: HashMap<String, utils::WikiDataFrame> = rdr_wiki.deserialize()
        .into_iter()
        .filter_map(|result: csv::Result<utils::WikiDataFrame>| result.map(|x| (x.language.clone(), x)).ok())
        .collect();

    let file = File::open(path)?;
    let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);

    let mut buffer = Vec::<NativeDataFrameRaw>::with_capacity(1000);
    let mut groups_hash: HashMap::<String, (utils::GroupBy, usize)> = HashMap::new();

    for record in rdr.deserialize().filter_map(Result::ok) {
        buffer.push(record);

        if buffer.len() < 1000 {
            continue;
        }

        process_buffer(&mut groups_hash, &mut buffer, &hash_wikipedia);
    }

    process_buffer(&mut groups_hash, &mut buffer, &hash_wikipedia);

    let groups = groups_hash
        .into_iter()
        .map(|(_, (mut group, count))| {
            group.reputation_at_post_creation /= count as f64;
            group.owner_undeleted_answer_count_at_post_time /= count as f64;
            group.imperative /= count as f64;
            group.object_oriented /= count as f64;
            group.functional /= count as f64;
            group.procedural /= count as f64;
            group.generic /= count as f64;
            group.reflective /= count as f64;
            group.event_driven /= count as f64;

            group
        });

    let mut wtr = csv::Writer::from_path(output_path)?;
    for record in groups {
        wtr.serialize(record)?;
    }

    println!("Overall time taken: {:?}", start.elapsed());

    Ok(())
}

fn process_buffer(
    groups_hash: &mut HashMap::<String, (utils::GroupBy, usize)>,
    buffer: &mut Vec<NativeDataFrameRaw>,
    hash_wikipedia: &HashMap<String, utils::WikiDataFrame>,
) {
    let data = buffer
        .drain(..)
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(NativeDataFrame::from)
        .collect::<Vec<_>>();

    for record in data {
        let (group, count) = groups_hash.entry(record.open_status.clone()).or_default();
        group.status = record.open_status;
        group.reputation_at_post_creation += record.reputation_at_post_creation;
        group.owner_undeleted_answer_count_at_post_time += record.owner_undeleted_answer_count_at_post_time;

        if let Some(wiki) = hash_wikipedia.get(&record.tag1) {
            group.imperative += wiki.imperative;
            group.object_oriented += wiki.object_oriented;
            group.functional += wiki.functional;
            group.procedural += wiki.procedural;
            group.generic += wiki.generic;
            group.reflective += wiki.reflective;
            group.event_driven += wiki.event_driven;
        }

        *count += 1;
    }
}

fn main() {
    let path = "train_October_9_2012.csv";
    let output_native_rust_path = "native_rust_optimized_output.csv";
    let path_wikipedia = "wikipedia.csv";

    use_native_parallel_rust(path, path_wikipedia, output_native_rust_path)
        .expect("Test of polar oriented result.");
}

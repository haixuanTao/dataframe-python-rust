mod utils;
use chrono::DateTime;
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::time::Instant;

fn use_native_rust(
    path: &str,
    path_wikipedia: &str,
    output_path: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let t_initial = Instant::now();

    let file = File::open(path)?;

    let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let mut records: Vec<utils::NativeDataFrame> = Vec::new();
    for result in rdr.deserialize() {
        match result {
            Ok(rec) => {
                records.push(rec);
            }
            Err(e) => println!("{}", e),
        };
    }

    let file = File::open(path_wikipedia)?;
    let mut rdr_wiki = csv::ReaderBuilder::new().delimiter(b',').from_reader(file);
    let mut records_wikipedia: Vec<utils::WikiDataFrame> = Vec::new();

    for result in rdr_wiki.deserialize() {
        match result {
            Ok(rec) => {
                records_wikipedia.push(rec);
            }
            Err(e) => println!("{}", e),
        };
    }

    let t_reading = Instant::now();

    // 1. Apply Format Date
    let fmt = "%m/%d/%Y %H:%M:%S";

    records
        .iter_mut()
        .for_each(|record: &mut utils::NativeDataFrame| {
            record.PostCreationDatetime =
                match DateTime::parse_from_str(record.PostCreationDate.as_ref().unwrap(), fmt) {
                    Ok(dates) => Some(dates),
                    Err(_) => None,
                }
        });

    let t_formatting = Instant::now();

    // 2. Apply Custom Formatting
    records
        .iter_mut()
        .for_each(|record: &mut utils::NativeDataFrame| {
            record.CountWords =
                Some(record.BodyMarkdown.as_ref().unwrap().split(' ').count() as f64)
        });

    let t_count_words = Instant::now();
    let hash_wikipedia: &HashMap<&String, &utils::WikiDataFrame> = &records_wikipedia
        .iter()
        .map(|record| (record.Language.as_ref().unwrap(), record))
        .collect();

    records.iter_mut().for_each(|record| {
        record.Wikipedia = match hash_wikipedia.get(&record.Tag1.as_ref().unwrap()) {
            Some(wikipedia) => Some(wikipedia.clone().clone()),
            None => None,
        }
    });

    let t_merging = Instant::now();

    let groups_hash: HashMap<String, (utils::GroupBy, i16)> = records
        .iter() // .par_iter()
        .fold(
            HashMap::new(), // || HashMap::new()
            |mut hash_group: HashMap<String, (utils::GroupBy, i16)>, record| {
                let group: utils::GroupBy = if let Some(wiki) = &record.Wikipedia {
                    utils::GroupBy {
                        status: record.OpenStatus.as_ref().unwrap().to_string(),
                        ReputationAtPostCreation: record.ReputationAtPostCreation.unwrap(),
                        OwnerUndeletedAnswerCountAtPostTime: record
                            .OwnerUndeletedAnswerCountAtPostTime
                            .unwrap(),
                        Imperative: wiki.Imperative.unwrap(),
                        ObjectOriented: wiki.ObjectOriented.unwrap(),
                        Functional: wiki.Functional.unwrap(),
                        Procedural: wiki.Procedural.unwrap(),
                        Generic: wiki.Generic.unwrap(),
                        Reflective: wiki.Reflective.unwrap(),
                        EventDriven: wiki.EventDriven.unwrap(),
                    }
                } else {
                    utils::GroupBy {
                        status: record.OpenStatus.as_ref().unwrap().to_string(),
                        ReputationAtPostCreation: record.ReputationAtPostCreation.unwrap(),
                        OwnerUndeletedAnswerCountAtPostTime: record
                            .OwnerUndeletedAnswerCountAtPostTime
                            .unwrap(),
                        ..Default::default()
                    }
                };
                if let Some((previous, count)) = hash_group.get_mut(&group.status.to_string()) {
                    *previous = previous.clone() + group;
                    *count += 1;
                } else {
                    hash_group.insert(group.status.to_string(), (group, 1));
                };
                hash_group
            },
        ); // }
           // .reduce(
           //     || HashMap::new(),
           //     |prev, other| {
           //         let set1: HashSet<String> = prev.keys().cloned().collect();
           //         let set2: HashSet<String> = other.keys().cloned().collect();
           //         let unions: HashSet<String> = set1.union(&set2).cloned().collect();
           //         let mut map = HashMap::new();
           //         for key in unions.iter() {
           //             map.insert(
           //                 key.to_string(),
           //                 match (prev.get(key), other.get(key)) {
           //                     (Some((previous, count_prev)), Some((group, count_other))) => {
           //                         (previous.clone() + group.clone(), count_prev + count_other)
           //                     }
           //                     (Some(previous), None) => previous.clone(),
           //                     (None, Some(other)) => other.clone(),
           //                     (None, None) => (utils::GroupBy::new(), 0),
           //                 },
           //             );
           //         }
           //         map
           //     },
           // );

    let groups: Vec<utils::GroupBy> = groups_hash
        .iter()
        .map(|(_, (group, count))| utils::GroupBy {
            status: group.status.to_string(),
            ReputationAtPostCreation: group.ReputationAtPostCreation / count.clone() as f64,
            OwnerUndeletedAnswerCountAtPostTime: group.OwnerUndeletedAnswerCountAtPostTime
                / count.clone() as f64,
            Imperative: group.Imperative / count.clone() as f64,
            ObjectOriented: group.ObjectOriented / count.clone() as f64,
            Functional: group.Functional / count.clone() as f64,
            Procedural: group.Procedural / count.clone() as f64,
            Generic: group.Generic / count.clone() as f64,
            Reflective: group.Reflective / count.clone() as f64,
            EventDriven: group.EventDriven / count.clone() as f64,
        })
        .collect();

    let t_groupby = Instant::now();

    let mut wtr = csv::Writer::from_path(output_path)?;

    for record in groups {
        wtr.serialize(record)?;
    }

    let t_writing = Instant::now();

    let _ = records
        .iter()
        .filter(|record| record.Tag1 == Some("rust".to_string()))
        .collect::<Vec<&utils::NativeDataFrame>>();

    let t_filtering = Instant::now();

    let timings = [
        t_initial,
        t_reading,
        t_formatting,
        t_count_words,
        t_merging,
        t_groupby,
        t_writing,
        t_filtering,
    ];
    let names = [
        "reading",
        "formatting",
        "count_words",
        "merging",
        "groupby",
        "writing",
        "filtering",
    ];
    for (i, name) in names.iter().enumerate() {
        println!("{}: {:#?}", name, (timings[i + 1] - timings[i]).as_millis());
    }

    Ok(())
}

fn main() {
    let path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/train_October_9_2012.csv";
    let output_native_rust_path =
        "/home/peter/Documents/TEST/RUST/stack-overflow/data/native_rust_output.csv";
    let path_wikipedia = "/home/peter/Documents/TEST/RUST/stack-overflow/data/wikipedia.csv";

    use_native_rust(path, path_wikipedia, output_native_rust_path)
        .expect("Test of polar oriented result.");
}

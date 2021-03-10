mod utils;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use rayon::prelude::*;
use std::fs::File;
use std::io::Cursor;
use std::time::{Duration, Instant};

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
        .par_iter_mut()
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
        .par_iter_mut()
        .for_each(|record: &mut utils::NativeDataFrame| {
            record.CountWords =
                Some(record.BodyMarkdown.as_ref().unwrap().split(' ').count() as f64)
        });

    let t_count_words = Instant::now();

    for record_wiki in records_wikipedia {
        records
            .par_iter_mut()
            .filter(|record| record.Tag1 == record_wiki.Language)
            .for_each(|x| {
                x.Wikipedia = Some(record_wiki.clone());
            });
    }

    let t_merging = Instant::now();

    let groups = &records
        .into_iter()
        .sorted_unstable_by(|a, b| Ord::cmp(&a.OpenStatus, &b.OpenStatus))
        .group_by(|record| record.OpenStatus.clone())
        .into_iter()
        .map(|(status, group)| {
            let (
                ReputationAtPostCreation,
                OwnerUndeletedAnswerCountAtPostTime,
                Imperative,
                ObjectOriented,
                Functional,
                Procedural,
                Generic,
                Reflective,
                EventDriven,
                count
            ) = group.into_iter().fold(
                (0., 0., 0., 0., 0., 0., 0., 0., 0., 0.),
                |(
                    ReputationAtPostCreation,
                    OwnerUndeletedAnswerCountAtPostTime,
                    Imperative,
                    ObjectOriented,
                    Functional,
                    Procedural,
                    Generic,
                    Reflective,
                    EventDriven,
                    count
                ),
                 record| {
                    if let Some(wiki) = record.Wikipedia {
                        (
                            ReputationAtPostCreation + record.ReputationAtPostCreation.unwrap(),
                            OwnerUndeletedAnswerCountAtPostTime + record.OwnerUndeletedAnswerCountAtPostTime.unwrap(),
                            Imperative + wiki.Imperative.unwrap(),
                            ObjectOriented + wiki.ObjectOriented.unwrap(),
                            Functional + wiki.Functional.unwrap(),
                            Procedural + wiki.Procedural.unwrap(),
                            Generic + wiki.Generic.unwrap(),
                            Reflective + wiki.Reflective.unwrap(),
                            EventDriven + wiki.EventDriven.unwrap(),
                            count + 1.,
                        )
                    } else {
                        (
                            ReputationAtPostCreation,
                            OwnerUndeletedAnswerCountAtPostTime,
                            Imperative,
                            ObjectOriented,
                            Functional,
                            Procedural,
                            Generic,
                            Reflective,
                            EventDriven,
                            count+1.,
                        )
                    }
                },
            );
            utils::GroupBy {
                status: status.unwrap(), 
                ReputationAtPostCreation: ReputationAtPostCreation/count,
                OwnerUndeletedAnswerCountAtPostTime: OwnerUndeletedAnswerCountAtPostTime/count,
                Imperative: Imperative/count,
                ObjectOriented: ObjectOriented/count,
                Functional: Functional/count,
                Procedural: Procedural/count,
                Generic: Generic/count,
                Reflective: Reflective/count,
                EventDriven: EventDriven/count,
            }
        })
        .collect::<Vec<utils::GroupBy>>();
    let t_groupby = Instant::now();

    let t_filtering = Instant::now();

    let mut wtr = csv::Writer::from_path(output_path)?;

    for record in groups {
        wtr.serialize(record)?;
    }
    let t_writing = Instant::now();

    let timings = [
        t_initial,
        t_reading,
        t_formatting,
        t_count_words,
        t_merging,
        t_groupby,
        t_filtering,
        t_writing,
    ];
    let names = [
        "reading",
        "formatting",
        "count_words",
        "merging",
        "groupby",
        "filtering",
        "writing",
    ];
    for (i, name) in names.iter().enumerate() {
        println!("{}: {:#?}", name, (timings[i + 1] - timings[i]).as_millis());
    }

    Ok(())
}

fn main() {
    let path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/train_October_9_2012.csv";
    let output_native_rust_path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/polars_eager_output.csv";
    let path_wikipedia = "/home/peter/Documents/TEST/RUST/stack-overflow/data/wikipedia.csv";

    use_native_rust(path, path_wikipedia, output_native_rust_path)
        .expect("Test of polar oriented result.");
}

use polars::prelude::*;
mod utils;
use std::fs::File;
use std::time::{Duration, Instant};

fn str_to_date(dates: &Series) -> std::result::Result<Series, PolarsError> {
    let fmt = Some("%m/%d/%Y %H:%M:%S");

    Ok(dates.utf8()?.as_date64(fmt)?.into_series())
}

fn count_words(dates: &Series) -> std::result::Result<Series, PolarsError> {
    Ok(dates
        .utf8()?
        .into_iter()
        .map(|opt_name: Option<&str>| opt_name.map(|name: &str| name.split(" ").count() as u64))
        .collect::<UInt64Chunked>()
        .into_series())
}

fn use_polars(
    path: &str,
    path_wikipedia: &str,
    output_path: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {

    let t_initial = Instant::now();

    let target_column = vec![
        "Language",
        "Imperative",
        "Object-oriented",
        "Functional",
        "Procedural",
        "Generic",
        "Reflective",
        "Event-driven",
    ];

    let mut df = CsvReader::from_path(path)?
        .with_encoding(CsvEncoding::LossyUtf8)
        // .with_n_threads(Some(1))
        .has_header(true)
        .finish()?;
    let mut df_wikipedia = CsvReader::from_path(path_wikipedia)?
        .with_encoding(CsvEncoding::LossyUtf8)
        // .with_n_threads(Some(1))
        .has_header(true)
        .finish()?
        .select(target_column)?;

    let t_reading = Instant::now();

    // 1. Apply Format Date
    df.may_apply("PostCreationDate", str_to_date)?;

    let t_formatting = Instant::now();

    // 2. Apply Custom Formatting
    df.may_apply("BodyMarkdown", count_words)?;

    let t_count_words = Instant::now();

    df = df
        .join(&df_wikipedia, "Tag1", "Language", JoinType::Left)?
        .fill_none(FillNoneStrategy::Min)?;

    let t_merging = Instant::now();

    // 3. groupby
    let groupby_series = vec![
        df.column("OpenStatus")?.clone(),
    ];

    let target_column = vec![
        "ReputationAtPostCreation",
        "OwnerUndeletedAnswerCountAtPostTime",
        "Imperative",
        "Object-oriented",
        "Functional",
        "Procedural",
        "Generic",
        "Reflective",
        "Event-driven",
    ];

    let groups = df
        .groupby_with_series(groupby_series, false)?
        .select(target_column)
        .mean()?;

    let t_groupby = Instant::now();

    // 4. Filtering
    let values = df.column("Tag1")?;
    let mask = values.eq("rust");
    df = df.filter(&mask)?;

    let t_filtering = Instant::now();

    let mut buffer = File::create(output_path)?;

    CsvWriter::new(&mut buffer)
        .has_headers(true)
        .finish(&mut groups.sort("OpenStatus", false)?)
        .expect("csv written");
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
    let output_polars_eager_path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/polars_eager_output.csv";
    let path_wikipedia = "/home/peter/Documents/TEST/RUST/stack-overflow/data/wikipedia.csv";

    use_polars(path, path_wikipedia, output_polars_eager_path).expect("Polar eager failed.");
}

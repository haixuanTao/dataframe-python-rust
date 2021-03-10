use polars::prelude::*;
mod utils;
use std::fs::File;

use std::time::{Duration, Instant};

fn lazy_str_to_date(dates: Series) -> std::result::Result<Series, PolarsError> {
    let fmt = Some("%m/%d/%Y %H:%M:%S");

    Ok(dates.utf8()?.as_date64(fmt)?.into_series())
}

fn lazy_date_to_hour(dates: Series) -> std::result::Result<Series, PolarsError> {
    Ok(dates.date64()?.hour().into_series())
}

fn lazy_count_words(dates: Series) -> std::result::Result<Series, PolarsError> {
    Ok(dates
        .utf8()?
        .into_iter()
        .map(|opt_name: Option<&str>| opt_name.map(|name: &str| name.split(" ").count() as u64))
        .collect::<UInt64Chunked>()
        .into_series())
}


fn use_lazy_polars(
    path: &str,
    path_wikipedia: &str,
    output_path: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let t_initial = Instant::now();
    let mut df_wikipedia = CsvReader::from_path(path_wikipedia)?
        .with_encoding(CsvEncoding::LossyUtf8)
        // .with_n_threads(Some(1))
        .has_header(true)
        .finish()?
        .lazy();

    let mut df = CsvReader::from_path(path)?
        .with_encoding(CsvEncoding::LossyUtf8)
        // .with_n_threads(Some(1))
        .has_header(true)
        .finish()?
        .lazy()
        .with_columns(vec![
            col("PostCreationDate")
                .map(lazy_str_to_date, Some(DataType::Date64))
                .map(lazy_date_to_hour, Some(DataType::Date64))
                .alias("hour"),
            col("BodyMarkdown")
                .map(lazy_count_words, Some(DataType::UInt64))
                .alias("newBodyMarkdown"),
        ])
        .inner_join(df_wikipedia, col("Tag1"), col("Language"), None)
        .groupby(vec![col("hour"), col("newBodyMarkdown")])
        .agg(vec![
            col("PostId").sum(),
            col("ReputationAtPostCreation").sum(),
        ])
        .select(&[
            col("hour"),
            col("newBodyMarkdown"),
            col("PostId_sum"),
            col("ReputationAtPostCreation_sum"),
        ])
        .sort("newBodyMarkdown", false)
        .collect()?;

    let mut buffer = File::create(output_path)?;

    CsvWriter::new(&mut buffer)
        .has_headers(true)
        .finish(&mut df)
        .expect("csv written");

    let t_writing = Instant::now();

    Ok(())
}


fn main() {
    let path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/train_October_9_2012.csv";
    let output_polars_lazy_path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/polars_lazy_output.csv";
    let path_wikipedia = "/home/peter/Documents/TEST/RUST/stack-overflow/data/wikipedia.csv";

    use_lazy_polars(path, path_wikipedia, output_polars_lazy_path).expect("Test of polar lazy failed.");
}

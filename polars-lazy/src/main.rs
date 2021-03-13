use polars::prelude::*;
use std::fs::File;
use std::time::Instant;

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
        .map(|opt_name: Option<&str>| opt_name.map(|name: &str| name.split(' ').count() as u64))
        .collect::<UInt64Chunked>()
        .into_series())
}

fn use_lazy_polars(
    path: &str,
    path_wikipedia: &str,
    output_path: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let t_initial = Instant::now();
    let df_wikipedia = LazyCsvReader::new(path_wikipedia.to_string())
        // .with_encoding(CsvEncoding::LossyUtf8)
        // .with_n_threads(Some(1))
        .has_header(true)
        .finish();

    let mut df = LazyCsvReader::new(path.to_string())
        // .with_encoding(CsvEncoding::LossyUtf8)
        // .with_n_threads(Some(1))
        .has_header(true)
        .finish()
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
        .groupby(vec![col("OpenStatus")])
        .agg(vec![
            col("ReputationAtPostCreation").mean(),
            col("OwnerUndeletedAnswerCountAtPostTime").mean(),
            col("Imperative").mean(),
            col("Object-oriented").mean(),
            col("Functional").mean(),
            col("Procedural").mean(),
            col("Generic").mean(),
            col("Reflective").mean(),
            col("Event-driven").mean(),
        ])
        .select(&[
            col("OpenStatus"),
            col("ReputationAtPostCreation_mean"),
            col("OwnerUndeletedAnswerCountAtPostTime_mean"),
            col("Imperative_mean"),
            col("Object-oriented_mean"),
            col("Functional_mean"),
            col("Procedural_mean"),
            col("Generic_mean"),
            col("Reflective_mean"),
            col("Event-driven_mean"),
        ])
        .sort("OpenStatus", false)
        .collect()?;

    let mut buffer = File::create(output_path)?;

    CsvWriter::new(&mut buffer)
        .has_headers(true)
        .finish(&mut df)
        .expect("csv written");

    let t_writing = Instant::now();
    println!("Read to write: {}", (t_writing - t_initial).as_millis());
    Ok(())
}

fn main() {
    let path = "/home/peter/Documents/TEST/RUST/stack-overflow/data/train_October_9_2012.csv";
    let output_polars_lazy_path =
        "/home/peter/Documents/TEST/RUST/stack-overflow/data/polars_lazy_output.csv";
    let path_wikipedia = "/home/peter/Documents/TEST/RUST/stack-overflow/data/wikipedia.csv";

    use_lazy_polars(path, path_wikipedia, output_polars_lazy_path)
        .expect("Test of polar lazy failed.");
}

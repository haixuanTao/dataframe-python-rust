# dataframe-python-rust

Comparing Polars vs Pandas vs Dask vs Rust native :)



To run:

## Download the data at:

- `train_October_9_2012,csv` at https://www.kaggle.com/c/predict-closed-questions-on-stack-overflow/data?select=train_October_9_2012.csv
- wikipedia.csv at https://en.wikipedia.org/wiki/Comparison_of_programming_languages with `python get_wikipedia_table.py`

### Polars Lazy

```bash
cd polars-lazy
cargo build --release
../target/release/polars-lazy
```

### Polars Eager

```bash
cd polars-eager
cargo build --release
../target/release/polars-eager
```

### Native rust

```bash
cd native-rust
cargo build --release
../target/release/native-rust
```

The result csv are going to be in data.

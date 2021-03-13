import dask.dataframe as dd
import pandas as pd
from io import StringIO
from datetime import datetime

t_initial = datetime.now()

# 1. Reading
PATH = "/home/peter/Documents/TEST/RUST/stack-overflow/data/train_October_9_2012.csv"

PATH_DASK = "/home/peter/Documents/TEST/RUST/stack-overflow/data/SO.csv"
PATH_WIKIPEDIA = (
    "/home/peter/Documents/TEST/RUST/stack-overflow/data/wikipedia.csv"
)
PATH_OUTPUT = (
    "/home/peter/Documents/TEST/RUST/stack-overflow/data/python_output.csv"
)
PATH_DASK_OUTPUT = (
    "/home/peter/Documents/TEST/RUST/stack-overflow/data/dask-output-*.csv"
)

df = pd.read_csv(
    PATH,
)

df = dd.from_pandas(df, npartitions=2)
df_wikipedia = dd.read_csv(PATH_WIKIPEDIA)

t_reading = datetime.now()

# 2. Formatting date
df["PostCreationDate"] = dd.to_datetime(
    df["PostCreationDate"], format="%m/%d/%Y %H:%M:%S"
)

t_formatting = datetime.now()

# 3. Formatting custom field
count_words = lambda x: len(x.split(" "))

df["BodyMarkdown"] = df["BodyMarkdown"].map(
    count_words,
)

t_count_words = datetime.now()

# 4. Merging
df = dd.merge(
    df, df_wikipedia, left_on="Tag1", right_on="Language", how="left"
).fillna(0)

t_merging = datetime.now()

# 4. Groupby
groupby_series = [df["OpenStatus"]]
target_column = [
    "ReputationAtPostCreation",
    "OwnerUndeletedAnswerCountAtPostTime",
    "Imperative",
    "Object-oriented",
    "Functional",
    "Procedural",
    "Generic",
    "Reflective",
    "Event-driven",
]

groups = df.groupby(by=groupby_series)[target_column].mean()

t_groupby = datetime.now()

# 5. Filtering
df = df[df["Tag1"] == "rust"]

t_filtering = datetime.now()

# 6. Writing

groups.compute().to_csv(PATH_DASK_OUTPUT)
t_writing = datetime.now()

# 7. printing

timings = [
    t_initial,
    t_reading,
    t_formatting,
    t_count_words,
    t_merging,
    t_groupby,
    t_filtering,
    t_writing,
]

names = [
    "reading",
    "formatting",
    "count_words",
    "merging",
    "groupby",
    "filtering",
    "writing",
]

for i, name in enumerate(names):

    print(f"{name}: {(timings[i+1] - timings[i]).total_seconds() * 1000}")

# df = dd.read_csv(
#     "partitions/*.csv",
#     dtype={
#         "OwnerUndeletedAnswerCountAtPostTime": "float64",
#         "OwnerUserId": "object",
#         "PostId": "object",
#         "ReputationAtPostCreation": "float64",
#         "Unnamed: 0": "object",
#     },
# )


# df.repartition(npartitions=100).to_csv("partitiions/*.csv", index=False)

# group = df.groupby(df.Tag1).ReputationAtPostCreation.sum().compute()
# group.to_csv("dask_output-*.csv")
# df["BodyMarkdown"] = df["BodyMarkdown"].str.replace("\r\n", " ")

# df["PostCreationDate"] = pd.to_datetime(
#     df["PostCreationDate"], format="%m/%d/%Y %H:%M:%S"
# )
# df["OwnerCreationDate"] = pd.to_datetime(df["OwnerCreationDate"], infer_datetime_format=True
# )

import pandas as pd

df = pd.read_html(
    "https://en.wikipedia.org/wiki/Comparison_of_programming_languages"
)[1]

# df = df.replace("Yes", 1)
# df = df.replace("No", 0)
df = df.fillna(0)

columns = [
    "Imperative",
    "Object-oriented",
    "Functional",
    "Procedural",
    "Generic",
    "Reflective",
    "Event-driven",
]

for col in columns:
    index = df[col].str.contains("Yes*") == True

    df.loc[index, col] = 1
    df.loc[~index, col] = 0

df["Language"] = df["Language"].str.lower()
df.to_csv("data/wikipedia.csv", index=False)

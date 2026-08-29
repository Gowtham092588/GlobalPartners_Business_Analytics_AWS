import pandas as pd
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
df = pd.read_csv('order_items.csv', sep=',', header=0)

print(df.head(10))
print(df.columns)
print(df.dtypes)
print(df.shape)
print(df.info())
print(df.notna().sum())
print(f"Duplicated Records: {df.duplicated().sum()}")
print(df.isnull().sum())
print(df.isna().sum())
print(df.describe())
print(
    df["LINEITEM_ID"]
    .astype("string")
    .str.strip()
    .eq("")
    .sum()
)

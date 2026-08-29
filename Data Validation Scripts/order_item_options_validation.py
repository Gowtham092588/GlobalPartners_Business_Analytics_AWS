import pandas as pd
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
df = pd.read_csv('order_item_options.csv', sep=',', header=0)

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
print("Total rows:", len(df))
print("Exact duplicates:", df.duplicated().sum())

print(
    "Business-key duplicates:",
    df.duplicated(
        subset=[
            "ORDER_ID",
            "LINEITEM_ID",
            "OPTION_GROUP_NAME",
            "OPTION_NAME"
        ]
    ).sum()
)
dup_rows = df[
    df.duplicated(
        subset=[
            "ORDER_ID",
            "LINEITEM_ID",
            "OPTION_GROUP_NAME",
            "OPTION_NAME",
            "OPTION_PRICE",
            "OPTION_QUANTITY"
        ],
        keep=False
    )
].sort_values(
    [
        "ORDER_ID",
        "LINEITEM_ID",
        "OPTION_GROUP_NAME",
        "OPTION_NAME",
        "OPTION_PRICE",
        "OPTION_QUANTITY"
    ]
)

print(dup_rows.head(10))

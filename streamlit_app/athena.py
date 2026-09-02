import boto3
import pandas as pd
import time


AWS_REGION = 'us-east-2'
ATHENA_DATABASE = 'globaldbgold'
ATHENA_OUTPUT_LOCATION = "s3://globalpartners-aws-data-bkt/results/"


def get_athena_client():

    athena = boto3.client("athena", region_name=AWS_REGION)

    return athena


def run_sql_query(athena, query):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )

    query_id = response["QueryExecutionId"]

    while True:

        status_response = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        status = (
            status_response["QueryExecution"]["Status"]["State"]
        )

        if status == 'QUEUED' or status == 'RUNNING':
            time.sleep(1)
        elif status == 'SUCCEEDED':
            return query_id
        else:
            error_message = status_response["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error")
            raise RuntimeError(f"Athena query failed {error_message}")


def get_results_df(athena, query_id):

    results = athena.get_query_results(
        QueryExecutionId=query_id
    )

    rows = results["ResultSet"]["Rows"]

    column_info = (
        results["ResultSet"]
        ["ResultSetMetadata"]
        ["ColumnInfo"]
    )

    if not rows:
        return pd.DataFrame()

    columns = [
        column["Name"]
        for column in column_info
    ]

    all_rows = rows[1:]

    next_token = results.get("NextToken")

    if not all_rows and not results.get("NextToken"):
        return pd.DataFrame(columns=columns)

    while next_token:

        results = athena.get_query_results(
            QueryExecutionId=query_id,
            NextToken=next_token
        )

        page_rows = results["ResultSet"]["Rows"]

        all_rows.extend(page_rows)

        next_token = results.get("NextToken")

    data = []

    for row in all_rows:

        values = [
            item.get("VarCharValue")
            for item in row["Data"]
        ]

        data.append(values)

    df = pd.DataFrame(
        data,
        columns=columns
    )

    df = convert_athena_types(
        df,
        column_info
    )

    return df


def convert_athena_types(df, column_info):

    for column in column_info:

        column_name = column["Name"]
        column_type = column["Type"]

        if column_type in [
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "float",
            "real",
            "double",
            "decimal"
        ]:
            df[column_name] = pd.to_numeric(
                df[column_name],
                errors="coerce"
            )
        elif column_type in ["date", "timestamp"]:
            df[column_name] = pd.to_datetime(
                df[column_name],
                errors="coerce"
            )
        elif column_type == "boolean":
            df[column_name] = (
                df[column_name]
                .str.lower()
                .map({
                    "true": True,
                    "false": False
                })
            )

        elif column_type in ["varchar", "char", "string"]:
            df[column_name] = df[column_name].astype("string")

    return df

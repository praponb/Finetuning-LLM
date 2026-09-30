from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-03-06").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.concat_ws("/", "account_type", "status").alias("account_type_status_key"),
    )
)

result = (
    cleaned
    .select("id", "status")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
        .select("id", "status")
    )
)

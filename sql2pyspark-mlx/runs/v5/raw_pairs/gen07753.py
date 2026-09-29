from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-27").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
    )
)

result = (
    base
    .alias("a")
    .join(df_accounts.alias("dim"), F.col("a.account_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.status").alias("status"),
        F.col("dim.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.amount")).alias("avg_amount"),
    )
)

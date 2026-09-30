from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-12-26 19:00:00").cast("timestamp"))
    .select(
        "*",
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("quarter", "txn_time").alias("quarter_start"))
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.min("txn_time").alias("first_txn_time"),
        F.min("amount").alias("min_amount"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("span_minutes").desc_nulls_first())
)

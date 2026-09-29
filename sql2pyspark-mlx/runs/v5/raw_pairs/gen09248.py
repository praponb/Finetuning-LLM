from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Starbucks"))))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.max("txn_time").alias("ended_at"),
    )
)

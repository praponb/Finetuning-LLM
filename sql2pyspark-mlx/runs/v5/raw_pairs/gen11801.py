from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") > F.lit("2023-06-08 06:00:00").cast("timestamp")))
    .withColumn("is_fee", F.when(F.col("txn_type") == "FEE", 1).otherwise(0))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("OTHER")))
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

filtered = (
    sessionized
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-10-07 07:00:00"))) & (F.col("amount") > 3500))
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

valid = (
    filtered
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-08-13 14:00:00"))) & F.col("is_flagged"))
    .withColumn("merchant_title", F.initcap(F.trim("merchant")))
)

remaining_keys = (
    valid
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(F.col("txn_type") == "FEE")
        .select("merchant", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    verified
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.min("txn_time").alias("first_seen"),
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
    )
    .filter(F.col("distinct_merchants") >= 1)
)

result = (
    grouped
    .select("quarter_start", "first_seen", "starbucks_events")
    .orderBy(F.col("starbucks_events").asc_nulls_last())
)

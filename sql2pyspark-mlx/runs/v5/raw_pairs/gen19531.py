from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("amount_bucket", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("last7_max_amount_bucket", "id").rowsBetween(-5, -1)
w8 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-12-10")
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_filled",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

metrics = (
    islands
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        "merchant_filled",
        F.ntile(10).over(w3).alias("amount_bucket"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("session_id").over(w5).alias("running_session_id"),
    )
)

metrics_transactions = (
    metrics
    .select(
        "id",
        "amount",
        "is_flagged",
        "amount_bucket",
        F.max("amount_bucket").over(w6).alias("last7_max_amount_bucket"),
        F.min("amount_bucket").over(w6).alias("last7_min_amount_bucket"),
        F.count("amount_bucket").over(w6).alias("last7_count_amount_bucket"),
    )
)

enriched = (
    metrics_transactions
    .select(
        "*",
        F.avg("last7_min_amount_bucket").over(w7).alias("last7_min_amount_bucket_avg_5_prior"),
        F.sum("amount_bucket").over(w8).alias("cumulative_amount_bucket"),
    )
)

result = (
    enriched
    .withColumn("last7_max_amount_bucket_tier", F.when(F.col("last7_max_amount_bucket") >= 8.8, "LARGE").otherwise("SMALL"))
    .withColumn("cumulative_amount_bucket_tier", F.when(F.col("cumulative_amount_bucket") >= 38, "HIGH").otherwise("LOW"))
    .rollup("last7_max_amount_bucket_tier", "cumulative_amount_bucket_tier")
    .agg(
        F.min("amount_bucket").alias("min_amount_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("last7_max_amount_bucket").alias("total_last7_max_amount_bucket"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cumulative_amount_bucket_tier").desc_nulls_last())
)

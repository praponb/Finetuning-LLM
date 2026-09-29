from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("amount", "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("merchant").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id", "merchant").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("txn_type") == "FEE", 1).otherwise(0).alias("is_fee"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

filtered = (
    sessions_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-06 00:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select("*", F.coalesce("is_fee", F.lit(0)).alias("is_fee_filled"))
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(5).over(w3).alias("amount_bucket"),
        F.avg("is_fee_filled").over(w4).alias("is_fee_filled_avg_5_prior"),
        F.last("is_fee_filled", ignorenulls=True).over(w5).alias("last_known_is_fee_filled"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.sum("last_known_is_fee_filled").over(w6).alias("cumulative_last_known_is_fee_filled"),
        F.sum("last_known_is_fee_filled").over(w7).alias("running_last_known_is_fee_filled"),
        F.avg("is_new_burst").over(w8).alias("account_id_avg_is_new_burst"),
    )
)

result = (
    enriched
    .cube(
        F.when(F.col("is_fee") >= 78, "LARGE").otherwise("SMALL").alias("is_fee_tier"),
        "session_no",
    )
    .agg(
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.percentile_approx("is_fee_filled", 0.5).alias("is_fee_filled_p50"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("avg_is_new_burst").asc_nulls_last())
)

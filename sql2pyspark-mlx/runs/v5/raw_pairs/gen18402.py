from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id")
w2 = Window.orderBy(F.col("txn_time_date").desc(), "id")
w3 = Window.orderBy("amount", "id")
w4 = Window.orderBy("event_seq", "id")
w5 = Window.orderBy(F.col("amount_bucket_2").asc_nulls_first())

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-06-10 07:00:00").cast("timestamp")) & (F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "*",
        F.to_date("txn_time").alias("txn_time_date"),
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

scored = (
    valid
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_type_merchant",
        F.ntile(10).over(w).alias("amount_bucket"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_windows = (
    scored
    .withColumn("next_amount_bucket", F.lead("amount_bucket", 1).over(w3))
    .withColumn("amount_bucket_2", F.ntile(3).over(w3))
)

scored_transactions = (
    with_windows
    .filter(F.col("amount_bucket_2") == 3)
    .select(
        "id",
        "amount_bucket",
        "next_amount_bucket",
        F.lag("amount_bucket_2").over(w4).alias("prev_amount_bucket_2"),
        F.lead("amount_bucket_2").over(w4).alias("next_amount_bucket_2"),
        F.lead("event_seq").over(w4).alias("next_event_seq"),
        F.lag("event_seq").over(w4).alias("prev_event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.cume_dist().over(w5).alias("amount_bucket_2_cume_dist"),
    )
)

result = (
    scored_transactions
    .rollup(
        F.when(F.col("next_event_seq") >= 7, "HIGH").otherwise("LOW").alias("next_event_seq_tier"),
        F.when(F.col("amount_bucket_2_cume_dist") >= 0.68, "LARGE").otherwise("SMALL").alias("amount_bucket_2_cume_dist_tier"),
        F.when(F.col("next_amount_bucket_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("next_amount_bucket_2_tier"),
    )
    .agg(
        F.percentile_approx("next_event_seq", 0.5).alias("next_event_seq_p50"),
        F.min("amount_bucket_2_cume_dist").alias("min_amount_bucket_2_cume_dist"),
        F.percentile_approx("amount_bucket_2_cume_dist", 0.95).alias("amount_bucket_2_cume_dist_p95"),
    )
    .filter(F.col("min_amount_bucket_2_cume_dist") >= 0)
)

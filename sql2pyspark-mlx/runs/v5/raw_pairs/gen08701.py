from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("amount_avg_3").asc_nulls_last())
w4 = Window.partitionBy("txn_type").orderBy("amount_avg_3", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("txn_type", "account_id").orderBy("amount_avg_3", "id")
w6 = Window.orderBy(F.col("amount_avg_3").desc(), "id")
w7 = Window.orderBy("amount_avg_3", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("account_id").orderBy("amount_avg_3_avg_3_prior", "id")
w9 = Window.partitionBy("txn_type").orderBy("prev_amount_avg_3", "id")
w10 = Window.partitionBy("merchant").orderBy(F.col("prev_amount_avg_3").desc(), "id")
w11 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount_avg_3_avg_3_prior").asc_nulls_first(), "id")
w12 = Window.partitionBy("next_amount_avg_3_avg_3_prior_tier").orderBy(F.col("next_amount_avg_3_avg_3_prior_p95").desc())

with_windows = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        F.first("merchant", ignorenulls=True).over(w).alias("first_known_merchant"),
        F.avg("amount").over(w2).alias("amount_avg_3"),
    )
)

enriched = (
    with_windows
    .withColumn("amount_avg_3_percent_rank", F.percent_rank().over(w3))
    .withColumn("amount_avg_3_avg_3_prior", F.avg("amount_avg_3").over(w4))
    .withColumn("prev_amount_avg_3", F.lag("amount_avg_3", 1).over(w5))
)

with_windows_transactions = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w6).alias("amount_avg_3_bucket"),
        F.avg("amount_avg_3_percent_rank").over(w7).alias("amount_avg_3_percent_rank_avg_3_prior"),
        F.lead("amount_avg_3_avg_3_prior", 2).over(w8).alias("next_amount_avg_3_avg_3_prior"),
        F.ntile(5).over(w9).alias("prev_amount_avg_3_bucket"),
        F.row_number().over(w10).alias("event_seq"),
    )
)

deduped = (
    with_windows_transactions
    .withColumn("dedup_rank", F.row_number().over(w11))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "amount_avg_3_percent_rank",
        "prev_amount_avg_3",
        "amount_avg_3_bucket",
        "next_amount_avg_3_avg_3_prior",
        "prev_amount_avg_3_bucket",
        "event_seq",
    )
)

result = (
    deduped
    .groupBy(
        F.when(F.col("next_amount_avg_3_avg_3_prior") >= 4382, "HIGH").otherwise("LOW").alias("next_amount_avg_3_avg_3_prior_tier"),
        F.when(F.col("prev_amount_avg_3_bucket") >= 8, "LARGE").otherwise("SMALL").alias("prev_amount_avg_3_bucket_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_amount_avg_3_avg_3_prior", 0.95).alias("next_amount_avg_3_avg_3_prior_p95"),
    )
    .withColumn("next_amount_avg_3_avg_3_prior_p95_percent_rank", F.percent_rank().over(w12))
)

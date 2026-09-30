from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("adjusted_amount", "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("adjusted_amount").desc())
w3 = Window.partitionBy("account_id").orderBy("adjusted_amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy(F.col("seq_no").desc_nulls_last())
w5 = Window.partitionBy("account_id").orderBy("seq_no", "id")
w6 = Window.partitionBy("account_id").orderBy(F.col("seq_no").asc_nulls_last(), "id")
w7 = Window.partitionBy("account_id")
w8 = Window.partitionBy("account_id").orderBy(F.col("running_max_next_adjusted_amount").desc(), "id")
w9 = Window.partitionBy("account_id").orderBy(F.col("running_avg_next_adjusted_amount").desc_nulls_last())
w10 = Window.partitionBy("account_id").orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)
w11 = Window.partitionBy("account_id").orderBy(F.col("account_id_min_running_avg_next_adjusted_amount").desc(), "id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-02 15:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "is_flagged",
        F.round((F.col("amount") * 0.9), 2).alias("adjusted_amount"),
    )
)

enriched = (
    base
    .select(
        "id",
        "account_id",
        "is_flagged",
        F.lead("adjusted_amount").over(w).alias("next_adjusted_amount"),
        F.row_number().over(w).alias("seq_no"),
        F.rank().over(w2).alias("adjusted_amount_rank"),
    )
)

scored = (
    enriched
    .filter(F.col("seq_no").between(3, 5))
    .withColumn("running_max_next_adjusted_amount", F.max("next_adjusted_amount").over(w3))
    .withColumn("running_count_next_adjusted_amount", F.count("next_adjusted_amount").over(w3))
    .withColumn("running_avg_next_adjusted_amount", F.avg("next_adjusted_amount").over(w3))
    .withColumn("running_min_next_adjusted_amount", F.min("next_adjusted_amount").over(w3))
)

scored_transactions = (
    scored
    .withColumn("seq_no_rank", F.rank().over(w4))
    .withColumn("seq_no_bucket", F.ntile(5).over(w5))
)

latest = (
    scored_transactions
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

scored_transactions_2 = (
    latest
    .withColumn("account_id_min_running_avg_next_adjusted_amount", F.min("running_avg_next_adjusted_amount").over(w7))
    .withColumn("seq_no_bucket_share", F.round((F.col("seq_no_bucket") / F.sum("seq_no_bucket").over(w7)), 4))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("running_avg_next_adjusted_amount_rank", F.rank().over(w9))
    .withColumn("adjusted_amount_rank_avg_5", F.avg("adjusted_amount_rank").over(w10))
)

latest_transactions = (
    scored_transactions_2
    .withColumn("row_num", F.row_number().over(w11))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_transactions
    .rollup("seq_no")
    .agg(
        F.percentile_approx("adjusted_amount_rank", 0.9).alias("adjusted_amount_rank_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("seq_no_bucket", 0.95).alias("seq_no_bucket_p95"),
        F.percentile_approx("running_avg_next_adjusted_amount_rank", 0.5).alias("running_avg_next_adjusted_amount_rank_p50"),
        F.percentile_approx("seq_no_bucket_share", [0.25, 0.5, 0.75]).alias("seq_no_bucket_share_quartiles"),
        F.percentile_approx("account_id_min_running_avg_next_adjusted_amount", 0.9).alias("account_id_min_running_avg_next_adjusted_amount_p90"),
    )
    .orderBy(F.col("distinct_ids").desc())
)

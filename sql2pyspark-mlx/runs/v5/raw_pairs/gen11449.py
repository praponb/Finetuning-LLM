from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("txn_type", "merchant").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("txn_type").orderBy(F.col("txn_time_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("amount_rank", "id")
w6 = Window.partitionBy("merchant")
w7 = Window.partitionBy("merchant").orderBy("amount_sum_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("txn_time_period", "id")
w9 = Window.partitionBy("merchant").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("merchant").orderBy("amount", "id")
w11 = Window.orderBy("txn_time_period", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("merchant").orderBy("merchant_stddev_samp_prev_amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("merchant").orderBy(F.col("burst_id").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.date_trunc("YEAR", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .filter(F.col("txn_time_period") >= "2023-10-01")
    .select(
        "id",
        "amount",
        "merchant",
        "txn_time_period",
        "merchant_filled",
        "latest_rank",
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.rank().over(w3).alias("amount_rank"),
        F.sum("amount").over(w4).alias("amount_sum_7d"),
    )
)

with_windows = (
    scored
    .withColumn("next_amount_rank", F.lead("amount_rank").over(w5))
    .withColumn("prev_amount_rank", F.lag("amount_rank").over(w5))
    .withColumn("next_amount", F.lead("amount").over(w5))
    .withColumn("row_seq", F.row_number().over(w5))
)

enriched = (
    with_windows
    .select(
        "id",
        "amount",
        "merchant",
        "txn_time_period",
        "merchant_filled",
        "amount_rank",
        "next_amount_rank",
        "next_amount",
        "row_seq",
        F.max("next_amount").over(w6).alias("merchant_max_next_amount"),
        F.stddev_samp("prev_amount_rank").over(w6).alias("merchant_stddev_samp_prev_amount_rank"),
        F.last("merchant", ignorenulls=True).over(w7).alias("last_known_merchant"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("txn_time_period") - F.lag("txn_time_period").over(w8) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "amount",
        "merchant",
        "txn_time_period",
        "amount_rank",
        "next_amount_rank",
        "row_seq",
        "merchant_stddev_samp_prev_amount_rank",
        "last_known_merchant",
        F.sum("is_new_session").over(w9).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .select(
        "*",
        F.row_number().over(w10).alias("row_seq_2"),
        F.avg("merchant_stddev_samp_prev_amount_rank").over(w11).alias("merchant_stddev_samp_prev_amount_rank_avg_2_prior"),
        F.ntile(10).over(w5).alias("amount_rank_bucket"),
        F.sum("amount_rank").over(w_grp).alias("running_amount_rank"),
        F.ntile(3).over(w_ordered).alias("burst_id_bucket"),
    )
)

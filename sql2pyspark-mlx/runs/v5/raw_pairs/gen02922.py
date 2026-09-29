from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.orderBy(F.col("amount").desc_nulls_first())

with_windows = (
    df_transactions
    .select(
        "*",
        F.count("amount").over(w).alias("amount_count_7d"),
        F.rank().over(w_running).alias("amount_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        "merchant",
        "account_id",
        F.when(F.col("amount") >= 4400, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount_rank", [0.25, 0.5, 0.75]).alias("amount_rank_quartiles"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
        F.avg(F.when(F.col("merchant") == "Starbucks", F.col("amount_count_7d"))).alias("starbucks_avg_amount_count_7d"),
    )
    .orderBy(F.col("amount_stddev").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("amount_sum_7d_plus_amount").desc())
w_ordered = Window.orderBy(F.col("amount_sum_7d_filled").desc_nulls_first())
w5 = Window.partitionBy("status")
w_hist = Window.partitionBy("method")
w7 = Window.orderBy(F.col("amount_sum_7d_filled_dense_rank").desc_nulls_first())
w8 = Window.partitionBy("status").orderBy(F.col("amount_sum_7d_filled_rank").desc_nulls_first())
w9 = Window.partitionBy("method", "status")

matched_keys = (
    df_payments
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.col("amount") > 590)
        .select("id", "status")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("status") == "FAILED")
    .select("*", F.lower("status").alias("status_lower"))
)

scored = (
    valid
    .withColumn("amount_percent_rank", F.percent_rank().over(w))
    .withColumn("amount_sum_7d", F.sum("amount").over(w2))
)

valid_payments = (
    scored
    .filter(F.col("paid_at") > F.lit("2024-04-18 08:00:00").cast("timestamp"))
    .select(
        "id",
        "method",
        "amount",
        "status",
        "status_lower",
        "amount_sum_7d",
        (F.col("amount_sum_7d") + F.col("amount")).alias("amount_sum_7d_plus_amount"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

cleaned = (
    valid_payments
    .filter(F.col("amount_sum_7d") <= 6982)
    .withColumn("method_prefix", F.substring("method", 1, 2))
    .withColumn("amount_sum_7d_filled", F.coalesce("amount_sum_7d", F.lit(0)))
)

with_windows = (
    cleaned
    .withColumn("amount_sum_7d_plus_amount_dense_rank", F.dense_rank().over(w3))
    .withColumn("amount_sum_7d_filled_rank", F.rank().over(w_ordered))
    .withColumn("amount_sum_7d_share", F.round((F.col("amount_sum_7d") / F.sum("amount_sum_7d").over(w5)), 4))
    .withColumn("amount_sum_7d_filled_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("method_stddev_samp_amount", F.stddev_samp("amount").over(w_hist))
)

scored_payments = (
    with_windows
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
        F.percent_rank().over(w7).alias("amount_sum_7d_filled_dense_rank_percent_rank"),
        F.rank().over(w8).alias("amount_sum_7d_filled_rank_rank"),
        F.avg("amount_sum_7d_plus_amount_dense_rank").over(w9).alias("method_avg_amount_sum_7d_plus_amount_dense_rank"),
    )
)

result = (
    scored_payments
    .select(
        "method",
        "status",
        "paid_at_date",
        "method_prefix",
        "amount_sum_7d_filled",
        "amount_sum_7d_plus_amount_dense_rank",
        "amount_sum_7d_share",
        "amount_sum_7d_filled_rank_rank",
        "method_avg_amount_sum_7d_plus_amount_dense_rank",
    )
)

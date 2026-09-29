from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("status_min_amount", "id")
w6 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last())
w7 = Window.partitionBy("method").orderBy("method_stddev_samp_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("next_status_min_amount").desc())
w_part = Window.partitionBy("status").orderBy("amount_count_30d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy("first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount", "id")
w11 = Window.partitionBy("method", "status")

metrics = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.stddev_samp("amount").over(w).alias("method_stddev_samp_amount"),
        F.last("method", ignorenulls=True).over(w2).alias("last_known_method"),
        F.min("amount").over(w3).alias("status_min_amount"),
        F.count("amount").over(w4).alias("amount_count_30d"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lead("status_min_amount", 1).over(w5).alias("next_status_min_amount"),
        F.rank().over(w6).alias("amount_rank"),
        F.first("method_stddev_samp_amount", ignorenulls=True).over(w7).alias("first_known_method_stddev_samp_amount"),
        F.round((F.col("status_min_amount") / F.sum("status_min_amount").over(w3)), 4).alias("status_min_amount_share"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

scored_payments = (
    ranked
    .select(
        "*",
        F.min("method_stddev_samp_amount").over(w_part).alias("running_min_method_stddev_samp_amount"),
        F.avg("method_stddev_samp_amount").over(w_part).alias("running_avg_method_stddev_samp_amount"),
        F.sum("method_stddev_samp_amount").over(w_part).alias("running_sum_method_stddev_samp_amount"),
        F.count("method_stddev_samp_amount").over(w_part).alias("running_count_method_stddev_samp_amount"),
    )
)

cleaned = (
    scored_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-07-11 15:00:00"))) & (F.col("status_min_amount_share") < 0.68))
    .select(
        "id",
        "method",
        "amount",
        "status",
        "last_known_method",
        "status_min_amount",
        "amount_rank",
        "status_min_amount_share",
        "running_min_method_stddev_samp_amount",
        "running_avg_method_stddev_samp_amount",
        "running_sum_method_stddev_samp_amount",
        F.round((F.col("first_known_method_stddev_samp_amount") / F.col("running_min_method_stddev_samp_amount")), 2).alias("first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount"),
        (F.col("next_status_min_amount") - F.col("running_min_method_stddev_samp_amount")).alias("next_status_min_amount_minus_running_min_method_stddev_samp_amount"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

with_windows = (
    cleaned
    .withColumn("next_amount_rank", F.lead("amount_rank").over(w10))
    .withColumn("next_first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount", F.lead("first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount").over(w10))
    .withColumn("row_seq", F.row_number().over(w10))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w11)), 4))
)

cleaned_payments = (
    with_windows
    .filter(F.col("method").isNotNull() & (F.col("running_avg_method_stddev_samp_amount") <= 1340))
    .select(
        "*",
        (F.col("amount_rank") + F.col("next_amount_rank")).alias("amount_rank_plus_next_amount_rank"),
    )
)

result = (
    cleaned_payments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.avg("amount_share").alias("avg_amount_share"),
        F.percentile_approx("amount_rank", [0.25, 0.5, 0.75]).alias("amount_rank_quartiles"),
        F.percentile_approx("running_sum_method_stddev_samp_amount", 0.5).alias("running_sum_method_stddev_samp_amount_p50"),
    )
    .filter(F.col("avg_amount_share") > 0)
    .orderBy(F.col("running_sum_method_stddev_samp_amount_p50").asc_nulls_last())
)

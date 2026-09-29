from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("running_min_amount_filled").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_recent = Window.orderBy("running_max_amount_filled_count_30d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_min_amount_filled_cume_dist_times_running_max_amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_max_amount_filled", "id")
w7 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.orderBy("running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d")
w9 = Window.orderBy(F.col("cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d").desc_nulls_last())
w10 = Window.orderBy("running_min_amount_filled_cume_dist_times_running_max_amount_filled", "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-08-01"))) & (~F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)

scored = (
    cleaned
    .filter(F.col("method") != "bank_transfer")
    .select(
        "id",
        "order_id",
        "paid_at",
        "paid_at_day",
        F.min("amount_filled").over(w).alias("running_min_amount_filled"),
        F.avg("amount_filled").over(w).alias("running_avg_amount_filled"),
        F.max("amount_filled").over(w).alias("running_max_amount_filled"),
    )
)

scored_payments = (
    scored
    .select(
        "id",
        "paid_at",
        "running_max_amount_filled",
        F.cume_dist().over(w2).alias("running_min_amount_filled_cume_dist"),
        F.count("running_max_amount_filled").over(w3).alias("running_max_amount_filled_count_30d"),
    )
)

filtered = (
    scored_payments
    .filter(F.col("paid_at") > F.lit("2023-06-15").cast("date"))
    .select(
        "*",
        (F.col("running_min_amount_filled_cume_dist") * F.col("running_max_amount_filled_count_30d")).alias("running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d"),
        (F.col("running_min_amount_filled_cume_dist") * F.col("running_max_amount_filled")).alias("running_min_amount_filled_cume_dist_times_running_max_amount_filled"),
    )
)

scored_payments_2 = (
    filtered
    .select(
        "*",
        F.sum("running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d").over(w_recent).alias("running_sum_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d"),
        F.avg("running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d").over(w_recent).alias("running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d"),
        F.min("running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d").over(w_recent).alias("running_min_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d"),
        F.sum("running_min_amount_filled_cume_dist").over(w_recent).alias("cumulative_running_min_amount_filled_cume_dist"),
    )
)

with_windows = (
    scored_payments_2
    .select(
        "id",
        "running_max_amount_filled",
        "running_max_amount_filled_count_30d",
        "running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d",
        "running_min_amount_filled_cume_dist_times_running_max_amount_filled",
        "running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d",
        "cumulative_running_min_amount_filled_cume_dist",
        F.sum("running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d").over(w5).alias("cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d"),
        F.lag("running_min_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d", 1).over(w6).alias("prev_running_min_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d"),
        F.count("running_min_amount_filled_cume_dist").over(w7).alias("running_min_amount_filled_cume_dist_count_30d"),
    )
)

enriched = (
    with_windows
    .withColumn("running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist", F.cume_dist().over(w8))
    .withColumn("cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist", F.cume_dist().over(w9))
    .withColumn("row_seq", F.row_number().over(w10))
)

result = (
    enriched
    .withColumn("running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_tier", F.when(F.col("running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d") >= 67, "LARGE").otherwise("SMALL"))
    .withColumn("cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist_tier", F.when(F.col("cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist") >= 0.75, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_tier", "cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist_tier"], ["running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_tier"], []],
        "running_avg_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_tier",
        "cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(
        F.col("cumulative_running_min_amount_filled_cume_dist_times_running_max_amount_filled_count_30d_cume_dist_tier").desc_nulls_last(),
    )
)

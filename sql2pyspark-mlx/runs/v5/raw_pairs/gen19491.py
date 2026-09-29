from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("status_length").desc_nulls_last())
w2 = Window.partitionBy("status", "method")
w3 = Window.orderBy("status_length_minus_latest_rank", "id")
w4 = Window.partitionBy("order_id").orderBy("status_length_minus_latest_rank", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("order_id").orderBy("status_length_minus_latest_rank", "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-09-19 01:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-09-13")))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "latest_rank",
        (F.col("status_length") - F.col("latest_rank")).alias("status_length_minus_latest_rank"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "order_id",
        "status",
        "latest_rank",
        "status_length_minus_latest_rank",
        "resolved_method",
        F.avg("status_length_minus_latest_rank").over(w2).alias("status_avg_status_length_minus_latest_rank"),
        F.ntile(4).over(w3).alias("status_length_minus_latest_rank_bucket"),
        F.avg("status_length_minus_latest_rank").over(w4).alias("status_length_minus_latest_rank_avg_2_prior"),
        F.ntile(3).over(w5).alias("status_length_minus_latest_rank_bucket_2"),
    )
)

base = (
    with_windows
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.ceil("status_avg_status_length_minus_latest_rank").alias("status_avg_status_length_minus_latest_rank_whole"),
        (F.col("latest_rank") + F.col("status_length_minus_latest_rank_bucket")).alias("latest_rank_plus_status_length_minus_latest_rank_bucket"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.status_length_minus_latest_rank",
        "s.resolved_method",
        "s.status_avg_status_length_minus_latest_rank",
        "s.status_length_minus_latest_rank_avg_2_prior",
        "s.status_length_minus_latest_rank_bucket_2",
        "s.status_avg_status_length_minus_latest_rank_whole",
        F.col("p.channel"),
        F.col("p.amount"),
        F.col("p.order_date"),
    )
)

result = (
    enriched
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.channel").alias("order_channel"),
        F.col("s.channel").alias("payment_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.amount")).alias("avg_amount"),
        F.min(F.col("s.status_length_minus_latest_rank")).alias("min_status_length_minus_latest_rank"),
        F.grouping_id().alias("gid"),
    )
)

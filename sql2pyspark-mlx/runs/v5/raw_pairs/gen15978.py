from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "method").orderBy(F.col("amount_filled").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id", "status").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("running_amount_filled", "id")

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

enriched = (
    valid
    .select(
        "*",
        F.sum("amount_filled").over(w).alias("running_amount_filled"),
        F.rank().over(w2).alias("amount_filled_rank"),
        F.lag("amount_filled", 2).over(w3).alias("prev_amount_filled"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "paid_at",
        "resolved_method",
        F.max("amount_filled_rank").over(w4).alias("running_max_amount_filled_rank"),
        F.min("amount_filled_rank").over(w4).alias("running_min_amount_filled_rank"),
        F.count("amount_filled_rank").over(w4).alias("running_count_amount_filled_rank"),
        F.ntile(10).over(w5).alias("running_amount_filled_bucket"),
    )
)

filtered = (
    scored
    .filter((F.col("paid_at") >= F.lit("2023-06-18").cast("date")) & (F.col("running_max_amount_filled_rank") >= 7.1))
    .select(
        "id",
        "order_id",
        "paid_at",
        "resolved_method",
        "running_max_amount_filled_rank",
        "running_min_amount_filled_rank",
        "running_count_amount_filled_rank",
        F.round((F.col("running_amount_filled_bucket") / F.col("running_count_amount_filled_rank")), 2).alias("running_amount_filled_bucket_per_running_count_amount_filled_rank"),
        F.coalesce("running_count_amount_filled_rank", F.lit(0)).alias("running_count_amount_filled_rank_filled"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["status", "channel"], ["status"], []],
        F.col("o.status").alias("status"),
        F.col("o.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.running_max_amount_filled_rank")).alias("total_running_max_amount_filled_rank"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)

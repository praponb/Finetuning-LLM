from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w_running_2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())
w_recent = Window.orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("channel", "order_id").orderBy("amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("amount_rank_2").desc(), "id")
w7 = Window.orderBy(F.col("seq_no").asc_nulls_last())
w8 = Window.partitionBy("status").orderBy("paid_at", "id")

with_windows = (
    df_payments
    .withColumn("amount_rank", F.rank().over(w_running))
    .withColumn("amount_rank_2", F.rank().over(w_running_2))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.order_date"), F.col("b.discount"), F.col("b.channel"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
        F.sum("discount").over(w5).alias("running_discount"),
    )
)

joined = (
    metrics
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.order_date").alias("order_order_date"))
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .withColumn("seq_no_percent_rank", F.percent_rank().over(w7))
    .withColumn("prev_seq_no", F.lag("seq_no", 2).over(w8))
)

summary = (
    enriched
    .groupBy(F.date_trunc("WEEK", "order_order_date").alias("week_start"))
    .agg(
        F.percentile_approx("running_discount", 0.5).alias("running_discount_p50"),
        F.round(F.sum("amount_rank_2") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_rank_2_per_failed"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
    )
)

result = (
    summary
    .select("week_start", "amount_rank_2_per_failed")
    .orderBy(F.col("amount_rank_2_per_failed").desc())
)

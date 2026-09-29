from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id").orderBy("discount", "id")
w3 = Window.partitionBy("channel").orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w6 = Window.orderBy("running_min_discount", "id")
w7 = Window.partitionBy("prev_running_min_discount_tier").orderBy(F.col("event_seq_p50").desc())

with_windows = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.sum("discount").over(w_part).alias("running_sum_discount"),
        F.min("discount").over(w_part).alias("running_min_discount"),
        F.avg("discount").over(w_part).alias("running_avg_discount"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

scored = (
    not_flagged
    .filter(F.col("running_avg_discount").isNotNull())
    .select(
        "id",
        "amount",
        "running_min_discount",
        F.avg("running_min_discount").over(w3).alias("running_min_discount_avg_5"),
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

metrics = (
    with_latest
    .select(
        "id",
        "running_min_discount",
        "running_min_discount_avg_5",
        "unit_price",
        F.lag("running_min_discount").over(w6).alias("prev_running_min_discount"),
        F.lag("running_min_discount_avg_5").over(w6).alias("prev_running_min_discount_avg_5"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_running_min_discount") >= 0.45, "LARGE").otherwise("SMALL").alias("prev_running_min_discount_tier"),
        F.when(F.col("running_min_discount") >= 0.37, "LARGE").otherwise("SMALL").alias("running_min_discount_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
    .withColumn("event_seq_p50_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("event_seq_p50").asc_nulls_last())
)

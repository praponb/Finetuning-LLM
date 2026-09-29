from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status", "store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("greatest_shipping_fee_discount").asc_nulls_last(), "id")
w3 = Window.partitionBy("store_id", "status").orderBy("discount", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("running_sum_discount").desc_nulls_first())
w5 = Window.partitionBy("status", "store_id").orderBy(F.col("shipping_fee").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.greatest("shipping_fee", "discount").alias("greatest_shipping_fee_discount"),
        F.concat("channel", F.lit(" - "), "status").alias("channel_status"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.count("discount").over(w_recent).alias("running_count_discount"),
        F.max("discount").over(w_recent).alias("running_max_discount"),
        F.sum("discount").over(w_recent).alias("running_sum_discount"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([14, 20])), "left_semi")
)

latest = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .filter(F.col("running_max_discount") >= 0.15)
    .select(
        "id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "channel_status",
        "running_count_discount",
        "running_max_discount",
        "running_sum_discount",
        "latest_rank",
        F.avg("running_count_discount").over(w3).alias("running_count_discount_avg_6_prior"),
        F.cume_dist().over(w4).alias("running_sum_discount_cume_dist"),
        F.ntile(10).over(w5).alias("shipping_fee_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        "channel",
        F.when(F.col("running_sum_discount") >= 1.94, "HIGH").otherwise("LOW").alias("running_sum_discount_tier"),
        F.when(F.col("running_count_discount") >= 17, "HIGH").otherwise("LOW").alias("running_count_discount_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "PENDING").alias("pending_events"),
        F.avg(F.when(F.col("channel") == "partner", F.col("running_count_discount"))).alias("partner_avg_running_count_discount"),
        F.percentile_approx("running_sum_discount", [0.25, 0.5, 0.75]).alias("running_sum_discount_quartiles"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .orderBy(F.col("partner_avg_running_count_discount").desc_nulls_first())
)

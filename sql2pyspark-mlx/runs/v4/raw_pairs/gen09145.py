from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("status").orderBy("quantity_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("channel").orderBy("unit_price", "id")
w7 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_order_items
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.channel"), F.col("d.shipping_fee"), F.col("d.status"))
)

scored = (
    with_dims
    .withColumn("cumulative_quantity_share", F.sum("quantity_share").over(w4))
    .withColumn("channel_stddev_samp_unit_price", F.stddev_samp("unit_price").over(w5))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w6))
    .withColumn("cumulative_corr_quantity_unit_price", F.sum("corr_quantity_unit_price").over(w7))
)

with_windows_order_items = (
    scored
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w3))
    .withColumn("last_known_quantity_share", F.last("quantity_share", ignorenulls=True).over(w8))
)

daily = (
    with_windows_order_items
    .groupBy("channel", "order_id")
    .agg(
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("shipping_fee_per_completed"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("status") == "PENDING", F.col("quantity"))).alias("pending_avg_quantity"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .filter(F.col("pending_avg_quantity") < 0)
)

result = (
    daily
    .select(
        "channel",
        "order_id",
        "shipping_fee_per_completed",
        "store_rows",
        "quantity_p90",
        F.when(F.col("channel").rlike("^(web|store)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(mobile|partner)$"), "CORE").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("channel").desc_nulls_first(), "shipping_fee_per_completed")
)

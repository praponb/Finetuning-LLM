from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_avg_discount", "id")
w4 = Window.partitionBy("customer_id")
w_ordered = Window.partitionBy("status").orderBy("running_min_discount", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .select(
        "*",
        F.count("discount").over(w).alias("running_count_discount"),
        F.max("discount").over(w).alias("running_max_discount"),
        F.min("discount").over(w).alias("running_min_discount"),
        F.avg("discount").over(w).alias("running_avg_discount"),
        F.first("discount", ignorenulls=True).over(w2).alias("first_known_discount"),
    )
)

with_windows_orders = (
    with_windows
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        "running_max_discount",
        "running_min_discount",
        "running_avg_discount",
        F.lag("running_count_discount", 2).over(w3).alias("prev_running_count_discount"),
        F.corr("discount", "shipping_fee").over(w4).alias("corr_discount_shipping_fee"),
        F.avg("amount").over(w_ordered).alias("amount_avg_6"),
    )
)

with_last = (
    with_windows_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_running_count_discount",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([9, 32])), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("order_item_unit_price") <= 180)
    .select(
        "*",
        F.round((F.col("order_item_unit_price") * 1.1), 2).alias("order_item_unit_price_adjusted"),
        F.floor("order_item_unit_price").alias("order_item_unit_price_whole"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("order_item_unit_price_whole") >= 72, "HIGH").otherwise("LOW").alias("order_item_unit_price_whole_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("order_item_unit_price", [0.25, 0.5, 0.75]).alias("order_item_unit_price_quartiles"),
    )
    .orderBy(F.col("order_item_unit_price_whole_tier").desc_nulls_last())
)

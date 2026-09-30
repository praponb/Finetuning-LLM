from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("discount").desc(), "id")
w_grp = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("shipping_fee_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("customer_id", "status")
w6 = Window.partitionBy("channel").orderBy("corr_shipping_fee_discount", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("discount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("corr_shipping_fee_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.year("order_date") >= 2024)
    .withColumn("shipping_fee_int", F.col("shipping_fee").cast("int"))
)

metrics = (
    valid
    .select(
        "id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.ntile(3).over(w2).alias("discount_bucket"),
        F.first("shipping_fee", ignorenulls=True).over(w_grp).alias("first_known_shipping_fee"),
        F.sum("shipping_fee_int").over(w_recent).alias("cumulative_shipping_fee_int"),
        F.corr("shipping_fee", "discount").over(w_recent_2).alias("corr_shipping_fee_discount"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") >= 360), "left_semi")
)

with_windows = (
    not_flagged
    .filter(F.col("shipping_fee") < 39)
    .select(
        "id",
        "amount",
        "shipping_fee",
        "discount_bucket",
        "first_known_shipping_fee",
        "corr_shipping_fee_discount",
        F.max("cumulative_shipping_fee_int").over(w6).alias("last3_max_cumulative_shipping_fee_int"),
        F.avg("cumulative_shipping_fee_int").over(w6).alias("last3_avg_cumulative_shipping_fee_int"),
        F.count("cumulative_shipping_fee_int").over(w6).alias("last3_count_cumulative_shipping_fee_int"),
        F.first("discount", ignorenulls=True).over(w7).alias("first_known_discount"),
    )
)

with_windows_orders = (
    with_windows
    .select(
        "id",
        "amount",
        "discount_bucket",
        "corr_shipping_fee_discount",
        "last3_max_cumulative_shipping_fee_int",
        "last3_count_cumulative_shipping_fee_int",
        F.avg("last3_avg_cumulative_shipping_fee_int").over(w8).alias("running_avg_last3_avg_cumulative_shipping_fee_int"),
        F.min("last3_avg_cumulative_shipping_fee_int").over(w8).alias("running_min_last3_avg_cumulative_shipping_fee_int"),
        F.max("last3_avg_cumulative_shipping_fee_int").over(w8).alias("running_max_last3_avg_cumulative_shipping_fee_int"),
        F.first("last3_count_cumulative_shipping_fee_int", ignorenulls=True).over(w9).alias("first_known_last3_count_cumulative_shipping_fee_int"),
    )
)

base = (
    with_windows_orders
    .filter(F.col("amount").between(550, 1290))
    .select(
        "id",
        "amount",
        "discount_bucket",
        "corr_shipping_fee_discount",
        "last3_count_cumulative_shipping_fee_int",
        "running_avg_last3_avg_cumulative_shipping_fee_int",
        "running_min_last3_avg_cumulative_shipping_fee_int",
        "running_max_last3_avg_cumulative_shipping_fee_int",
        (F.col("last3_max_cumulative_shipping_fee_int") + F.col("running_min_last3_avg_cumulative_shipping_fee_int")).alias("last3_max_cumulative_shipping_fee_int_plus_running_min_last3_avg_cumulative_shipping_fee_int"),
    )
)

result = (
    base
    .select(
        "id",
        "amount",
        "discount_bucket",
        "corr_shipping_fee_discount",
        "last3_count_cumulative_shipping_fee_int",
        "running_avg_last3_avg_cumulative_shipping_fee_int",
        "running_min_last3_avg_cumulative_shipping_fee_int",
        "running_max_last3_avg_cumulative_shipping_fee_int",
        "last3_max_cumulative_shipping_fee_int_plus_running_min_last3_avg_cumulative_shipping_fee_int",
    )
)

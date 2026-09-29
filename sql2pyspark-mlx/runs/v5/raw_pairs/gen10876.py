from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "order_id").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_channel").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)

joined = (
    df_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        F.col("o.shipping_fee"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.channel"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.channel").alias("order_channel"), F.col("d.discount"))
)

cleaned = (
    with_dims
    .filter(F.col("order_amount") >= 1420)
    .withColumn("channel_method", F.concat("channel", F.lit("/"), "method"))
    .withColumn("method_length", F.length("method"))
)

with_windows = (
    cleaned
    .filter(F.col("channel").isNull())
    .select(
        "id",
        "shipping_fee",
        "order_amount",
        "discount",
        F.count("discount").over(w).alias("running_count_discount"),
        F.min("discount").over(w).alias("running_min_discount"),
        F.max("discount").over(w).alias("running_max_discount"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_3"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("shipping_fee_avg_3") >= 40, "LARGE").otherwise("SMALL").alias("shipping_fee_avg_3_tier"),
    )
    .agg(
        F.sum("running_max_discount").alias("total_running_max_discount"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("total_running_max_discount").desc_nulls_last())
)

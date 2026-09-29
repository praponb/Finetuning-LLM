from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("cumulative_unit_price_whole", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy(F.col("first_known_unit_price_whole").desc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.floor("unit_price").alias("unit_price_whole"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.shipping_fee"))
)

scored = (
    joined
    .select(
        "*",
        F.first("unit_price_whole", ignorenulls=True).over(w).alias("first_known_unit_price_whole"),
        F.stddev_samp("shipping_fee").over(w2).alias("order_id_stddev_samp_shipping_fee"),
        F.sum("unit_price_whole").over(w3).alias("cumulative_unit_price_whole"),
        F.ntile(4).over(w4).alias("shipping_fee_bucket"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.avg("unit_price_whole").over(w5).alias("unit_price_whole_avg_6"),
        F.avg("unit_price").over(w2).alias("order_id_avg_unit_price"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "shipping_fee",
        "first_known_unit_price_whole",
        "order_id_stddev_samp_shipping_fee",
        "unit_price_whole_avg_6",
        "order_id_avg_unit_price",
    )
    .orderBy(F.col("unit_price_whole_avg_6").asc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("unit_price", "id")
w_ordered = Window.partitionBy("order_id", "product_id")
w3 = Window.partitionBy("product_id").orderBy("order_id_max_quantity", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("next_quantity").asc_nulls_last())
w5 = Window.partitionBy("product_id").orderBy(F.col("shipping_fee").desc_nulls_last())

scored = (
    df_order_items
    .select(
        "*",
        F.lead("quantity", 1, 0).over(w_hist).alias("next_quantity"),
        F.lag("quantity", 1).over(w_hist).alias("prev_quantity"),
        F.max("quantity").over(w_ordered).alias("order_id_max_quantity"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.channel"), F.col("d.discount"), F.col("d.shipping_fee"))
)

with_windows = (
    joined
    .select(
        "*",
        F.lead("shipping_fee", 1).over(w3).alias("next_shipping_fee"),
        F.percent_rank().over(w4).alias("next_quantity_percent_rank"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.stock_qty"), F.col("b.launch_date"), F.col("b.price"))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

totals = (
    deduped
    .groupBy(
        F.when(F.col("order_id_max_quantity") >= 9, "LARGE").otherwise("SMALL").alias("order_id_max_quantity_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("prev_quantity") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("prev_quantity_per_partner"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("shipping_fee_per_mobile"),
        F.round(F.avg("discount"), 2).alias("avg_discount_2"),
    )
    .filter(F.col("avg_discount") < 0)
)

result = (
    totals
    .select(
        "order_id_max_quantity_tier",
        "distinct_ids",
        "prev_quantity_per_partner",
        "avg_discount",
        "shipping_fee_per_mobile",
        "avg_discount_2",
    )
    .orderBy(
        F.col("prev_quantity_per_partner").desc_nulls_first(),
        "order_id_max_quantity_tier",
    )
)

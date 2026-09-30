from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("order_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w5 = Window.partitionBy("channel").orderBy(F.col("order_id_max_unit_price").desc_nulls_first(), "id")
w6 = Window.partitionBy("order_id", "order_channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("rank_in_group", "id")
w8 = Window.partitionBy("order_id", "channel").orderBy("quantity", "id")

enriched = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.order_date"))
)

metrics = (
    enriched
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4))
    .withColumn("order_id_max_unit_price", F.max("unit_price").over(w3))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w3))
    .withColumn("unit_price_bucket", F.ntile(5).over(w4))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.shipping_fee"), F.col("b.amount"), F.col("b.channel"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.discount"), F.col("p.channel").alias("order_channel"))
)

with_windows = (
    joined
    .select(
        "*",
        F.first("shipping_fee", ignorenulls=True).over(w6).alias("first_known_shipping_fee"),
        F.lag("unit_price_share", 1).over(w7).alias("prev_unit_price_share"),
        F.ntile(5).over(w8).alias("quantity_bucket"),
    )
)

stats = (
    with_windows
    .rollup(
        F.when(F.col("first_known_shipping_fee") >= 39, "PREMIUM").otherwise("STANDARD").alias("first_known_shipping_fee_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

result = (
    stats
    .select("first_known_shipping_fee_tier", "week_start", "store_rows", "distinct_ids")
    .orderBy(F.col("store_rows").asc_nulls_first())
)

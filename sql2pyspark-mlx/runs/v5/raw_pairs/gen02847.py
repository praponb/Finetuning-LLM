from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")
w3 = Window.partitionBy("order_id", "channel").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("quantity") < 3)
    .select(
        "*",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id")
)

enriched = (
    ranked
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_id").isNull())
    .select("s.*", F.col("p.discount"), F.col("p.shipping_fee"), F.col("p.channel"))
)

ranked_order_items = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched_order_items = (
    ranked_order_items
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.channel").alias("order_channel"),
        F.col("p.shipping_fee").alias("order_shipping_fee"),
        F.col("p.amount"),
    )
)

scored = (
    enriched_order_items
    .select(
        "id",
        "order_id",
        "amount",
        F.sum("discount").over(w3).alias("last4_sum_discount"),
        F.count("discount").over(w3).alias("last4_count_discount"),
        F.min("discount").over(w3).alias("last4_min_discount"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("amount") >= 1430, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.min("last4_min_discount").alias("min_last4_min_discount"),
        F.count("*").alias("row_count"),
        F.max("last4_sum_discount").alias("max_last4_sum_discount"),
        F.count("*").alias("cnt"),
        F.percentile_approx("last4_min_discount", 0.9).alias("last4_min_discount_p90"),
        F.percentile_approx("last4_count_discount", 0.95).alias("last4_count_discount_p95"),
    )
    .filter(F.col("last4_count_discount_p95") < 1000000)
    .orderBy(F.col("cnt").asc_nulls_first())
)

from pyspark.sql import functions as F

joined = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee") < 18)
    .select("e.*", F.col("o.order_date"), F.col("o.discount"), F.col("o.channel"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("quantity") >= 8, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("discount"))).alias("web_total_discount"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy("quantity_bit_or")
)

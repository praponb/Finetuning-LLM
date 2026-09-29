from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.status"), F.col("o.order_date"))
)

result = (
    enriched
    .groupBy("product_id")
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)

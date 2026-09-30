from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 300)
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

result = (
    cleaned
    .groupBy("order_id", "product_id")
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.count("*").alias("event_count"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .filter(F.col("quantity_bit_or") < 1)
)

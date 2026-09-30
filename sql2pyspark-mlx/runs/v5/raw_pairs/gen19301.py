from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 160) | (F.col("unit_price") >= 250))
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

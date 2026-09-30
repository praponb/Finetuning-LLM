from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 8) | F.col("unit_price").between(140, 250))
    .groupBy("product_id", "order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("unit_price").alias("max_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") < 2)
    .drop("unit_price_count")
)

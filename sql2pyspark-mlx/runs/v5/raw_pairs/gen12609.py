from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 20]) & F.col("product_id").isNotNull() & (F.col("quantity") < 5))
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
)

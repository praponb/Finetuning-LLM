from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("num_order_items") < 5)
    .orderBy(F.col("order_id").desc())
)

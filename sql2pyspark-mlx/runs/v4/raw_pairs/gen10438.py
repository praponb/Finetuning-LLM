from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .orderBy(F.col("order_id").desc())
)

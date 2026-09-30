from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 8)
    .groupBy("order_id", "product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .filter(F.col("avg_unit_price") > 67)
    .orderBy("unique_products", "order_id", "product_id")
    .limit(10)
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("unit_price").alias("unique_products"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("avg_quantity") >= 30)
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

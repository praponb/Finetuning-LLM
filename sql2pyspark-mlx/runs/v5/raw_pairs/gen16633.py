from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

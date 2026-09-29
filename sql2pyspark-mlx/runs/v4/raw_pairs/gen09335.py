from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

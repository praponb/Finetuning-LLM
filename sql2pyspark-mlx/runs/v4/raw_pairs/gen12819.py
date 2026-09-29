from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.sum("quantity").alias("total_quantity"),
    )
)

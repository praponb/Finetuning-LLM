from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([7, 8]))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 210)
)

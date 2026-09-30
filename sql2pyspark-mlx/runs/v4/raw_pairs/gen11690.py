from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 146)
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") < 220)
    .orderBy("min_last_restock")
)

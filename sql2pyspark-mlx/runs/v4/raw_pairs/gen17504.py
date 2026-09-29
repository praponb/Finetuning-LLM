from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("min_quantity") >= 200)
    .orderBy(F.col("product_id").desc())
)

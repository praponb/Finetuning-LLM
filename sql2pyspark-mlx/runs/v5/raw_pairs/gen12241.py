from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 210) | (F.col("last_restock") > "2024-08-01"))
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .orderBy(F.col("product_id").desc(), "last_last_restock")
)

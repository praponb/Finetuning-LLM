from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & (F.col("last_restock") < "2024-10-01"))
)

inventory_stats = (
    base
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("total_quantity") < 1310)
    .orderBy(F.col("product_id").desc())
)

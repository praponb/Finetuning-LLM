from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2025)
    .rollup(
        F.when(F.col("quantity") >= 196, "high").when(F.col("quantity") >= 92, "mid").otherwise("low").alias("quantity_bucket"),
        F.year("last_restock").alias("last_restock_year"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.max("quantity").alias("max_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

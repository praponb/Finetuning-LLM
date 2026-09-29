from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .groupBy(
        F.when(F.col("quantity") >= 183, "high").when(F.col("quantity") >= 100, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .orderBy(F.col("quantity_bucket").desc())
)

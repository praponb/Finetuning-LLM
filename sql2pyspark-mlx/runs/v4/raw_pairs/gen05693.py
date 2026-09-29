from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 3)
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 257, "high").when(F.col("quantity") >= 49, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 61)
    .orderBy(F.col("quantity_bucket").desc())
)

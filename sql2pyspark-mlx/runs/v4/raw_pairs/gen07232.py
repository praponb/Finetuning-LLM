from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 8) & ((F.col("product_id") == 6) | F.col("store_id").isin([4, 5])))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 211, "high").when(F.col("quantity") >= 36, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

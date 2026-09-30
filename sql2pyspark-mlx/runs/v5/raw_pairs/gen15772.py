from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 6]) & F.col("store_id").isin([2, 3, 4, 6]) & (F.col("last_restock") >= "2023-06-11")))
    .groupBy(
        F.when(F.col("quantity") >= 233, "high").when(F.col("quantity") >= 118, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy("quantity_bucket")
)

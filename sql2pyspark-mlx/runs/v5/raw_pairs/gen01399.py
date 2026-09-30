from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 241, "high").when(F.col("quantity") >= 33, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy("quantity_bucket")
    .limit(10)
)

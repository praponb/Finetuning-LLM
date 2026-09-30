from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 240, "high").when(F.col("quantity") >= 75, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 237, "high").when(F.col("quantity") >= 103, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .filter(F.col("cnt") < 1)
)

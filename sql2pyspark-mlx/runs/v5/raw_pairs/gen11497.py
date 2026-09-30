from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 172)
    .groupBy(
        F.when(F.col("quantity") >= 260, "high").when(F.col("quantity") >= 31, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") < 100)
    .orderBy("quantity_bucket", F.col("store_id").desc())
)

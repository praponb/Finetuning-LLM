from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 91, "high").when(F.col("cost") >= 22, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("flash_cost") >= 37)
    .orderBy(F.col("cost_count").desc())
)

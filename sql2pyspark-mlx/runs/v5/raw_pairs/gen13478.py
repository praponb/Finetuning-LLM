from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 83, "high").when(F.col("cost") >= 25, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.count("*").alias("num_shipments"),
    )
    .orderBy(F.col("cost_bucket").desc())
)

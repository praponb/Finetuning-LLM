from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 86, "high").when(F.col("cost") >= 28, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("avg_cost") < 63)
    .select("cost_bucket", "avg_cost", "cost_count")
)

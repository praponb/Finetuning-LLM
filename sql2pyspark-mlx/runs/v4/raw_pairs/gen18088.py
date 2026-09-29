from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 99, "high").when(F.col("cost") >= 49, "mid").otherwise("low").alias("cost_bucket"),
        "status",
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

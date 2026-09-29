from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        "carrier",
        F.when(F.col("cost") >= 82, "high").when(F.col("cost") >= 23, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy("status", "carrier", "cost_bucket")
    .limit(5)
)

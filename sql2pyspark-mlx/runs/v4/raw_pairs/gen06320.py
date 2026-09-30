from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .groupBy(
        F.when(F.col("cost") >= 78, "high").when(F.col("cost") >= 32, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
)

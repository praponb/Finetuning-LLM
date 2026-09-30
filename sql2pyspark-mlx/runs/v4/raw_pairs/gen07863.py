from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 73, "high").when(F.col("cost") >= 49, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") >= 66)
    .select("cost_bucket", "cost_range")
)

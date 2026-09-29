from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 81, "high").when(F.col("cost") >= 30, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") >= 33)
    .select("cost_bucket", "max_cost")
)

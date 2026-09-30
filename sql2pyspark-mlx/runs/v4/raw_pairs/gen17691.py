from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .groupBy(
        F.when(F.col("cost") >= 780, "high").when(F.col("cost") >= 150, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

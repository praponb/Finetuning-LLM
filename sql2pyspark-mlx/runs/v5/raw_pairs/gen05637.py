from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3100000, "high").when(F.col("budget") >= 1225000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("stddev_budget") < 48)
    .orderBy("unique_locations", "budget_bucket")
    .limit(3)
)

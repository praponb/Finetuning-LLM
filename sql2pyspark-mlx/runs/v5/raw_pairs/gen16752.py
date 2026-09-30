from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .groupBy("location")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.max("budget").alias("max_budget"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("avg_budget") > 54)
    .orderBy(F.col("max_budget").desc())
)

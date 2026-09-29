from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Chiang Mai", "Berlin"]))
    .groupBy(
        F.when(F.col("budget") >= 3806000, "high").when(F.col("budget") >= 1170000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.avg("budget").alias("avg_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy(F.col("bangkok_count").desc())
)

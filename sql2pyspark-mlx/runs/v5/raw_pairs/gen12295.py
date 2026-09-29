from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4271000, "high").when(F.col("budget") >= 1079000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.countDistinct("budget").alias("unique_locations"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") > 4)
    .drop("bangkok_count")
    .orderBy("avg_budget")
)

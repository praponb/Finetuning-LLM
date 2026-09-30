from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("budget") < 2225000)

departments_stats = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4403000, "high").when(F.col("budget") >= 898000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
)

result = departments_stats.filter(F.col("budget_count") >= 6)

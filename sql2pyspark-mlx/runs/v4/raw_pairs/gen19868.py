from pyspark.sql import functions as F

base = df_departments.filter(F.col("location") == "Singapore")

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3817000, "high").when(F.col("budget") >= 1974000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
)

result = (
    departments_stats
    .select("budget_bucket", "budget_count", "hr_count")
    .orderBy(F.col("budget_count").desc())
)

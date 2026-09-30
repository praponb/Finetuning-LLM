from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name") == "Sales")

budget_bucket_summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3419000, "high").when(F.col("budget") >= 1261000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)

result = budget_bucket_summary.orderBy("budget_bucket")

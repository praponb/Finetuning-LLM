from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3048000, "high").when(F.col("budget") >= 900000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("department_count"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
    )
    .filter(F.col("department_count") >= 8)
    .select("location", "budget_bucket", "department_count", "finance_count")
)

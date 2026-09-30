from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4182000, "high").when(F.col("budget") >= 1637000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"))
    .filter(F.col("hr_count") >= 25)
    .orderBy(F.col("dept_name").desc(), "budget_bucket")
)

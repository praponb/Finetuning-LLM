from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4244000, "high").when(F.col("budget") >= 808000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
    .orderBy(F.col("budget_bucket").desc())
)

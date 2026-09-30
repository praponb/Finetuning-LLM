from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3472000, "high").when(F.col("budget") >= 698000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
)

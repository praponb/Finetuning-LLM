from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3714000, "high").when(F.col("budget") >= 2056000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
    .orderBy(F.col("location").desc(), "finance_count")
)

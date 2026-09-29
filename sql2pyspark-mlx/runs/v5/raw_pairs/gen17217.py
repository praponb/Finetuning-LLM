from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1374000, 2222000))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4195000, "high").when(F.col("budget") >= 733000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("finance_budget") >= 61)
    .orderBy(F.col("location").desc())
)

from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["HR", "Finance"]) & F.col("dept_name").isin(["Finance", "Legal", "Sales"]))
    .groupBy(
        F.when(F.col("budget") >= 3888000, "high").when(F.col("budget") >= 1669000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
    )
    .orderBy(F.col("new_york_budget").desc())
)

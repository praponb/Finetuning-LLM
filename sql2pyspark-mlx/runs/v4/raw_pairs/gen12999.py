from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "New York", "London"]) | (F.col("dept_name") != "Legal"))
    .groupBy(
        F.when(F.col("budget") >= 3107000, "high").when(F.col("budget") >= 643000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") > 2)
    .orderBy(F.col("budget_count").desc(), F.col("budget_bucket").desc())
)

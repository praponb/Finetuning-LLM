from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok"]))
    .groupBy(
        F.when(F.col("budget") >= 3360000, "high").when(F.col("budget") >= 1170000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("total_budget") < 7518000)
)

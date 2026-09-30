from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") == "HR"))
    .groupBy(
        F.when(F.col("budget") >= 3069000, "high").when(F.col("budget") >= 1297000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
)

from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2097000)
    .groupBy("dept_name", "location")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0))) / F.count("*")), 1).alias("pct_hr"),
    )
    .filter(F.col("pct_hr") < 54)
)

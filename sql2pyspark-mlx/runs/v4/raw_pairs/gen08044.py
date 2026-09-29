from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
        F.sum("budget").alias("total_budget"),
    )
)

from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name") == "Sales")

summary = (
    recent_departments
    .groupBy("dept_name")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum("budget").alias("total_budget"),
    )
)

result = summary

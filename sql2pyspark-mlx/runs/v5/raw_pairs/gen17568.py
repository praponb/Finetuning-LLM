from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name").isin(["Sales", "Engineering"]))

summary = (
    valid_departments
    .groupBy("location")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum("budget").alias("total_budget"),
    )
)

result = summary.orderBy("total_budget")

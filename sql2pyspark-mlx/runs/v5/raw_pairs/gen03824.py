from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.min("budget").alias("min_budget"),
    )
)

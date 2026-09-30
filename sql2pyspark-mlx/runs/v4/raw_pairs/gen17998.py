from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") < 3185000)
    .orderBy("avg_budget")
)

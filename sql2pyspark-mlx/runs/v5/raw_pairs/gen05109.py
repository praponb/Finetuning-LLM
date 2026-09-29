from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .withColumn("budget_int", F.col("budget").cast("int"))
)

departments_stats = (
    recent_departments
    .groupBy("location")
    .agg(F.min("budget").alias("min_budget"))
)

result = departments_stats.orderBy("min_budget")

from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2537000)
    .groupBy("location", "dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("location") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .filter(F.col("new_york_count") >= 45)
    .drop("new_york_count")
)

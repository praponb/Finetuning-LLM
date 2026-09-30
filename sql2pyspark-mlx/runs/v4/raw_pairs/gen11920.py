from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.avg("budget").alias("avg_budget"),
    )
    .orderBy("dept_name")
)

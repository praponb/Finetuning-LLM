from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
    )
    .orderBy("dept_name")
)

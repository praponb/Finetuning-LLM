from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") | (F.col("location") != "Singapore"))
    .groupBy("dept_name")
    .agg(F.min("budget").alias("min_budget"))
)

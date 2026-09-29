from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3332000)
    .groupBy("dept_name")
    .agg(F.count("budget").alias("budget_count"))
    .orderBy(F.col("budget_count").desc(), "dept_name")
    .limit(20)
)

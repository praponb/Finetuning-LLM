from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .agg(F.count("budget").alias("budget_count"))
    .orderBy(F.col("budget_count").desc())
)

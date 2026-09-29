from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.min("budget").alias("min_budget"))
    .orderBy(F.col("min_budget").desc())
)

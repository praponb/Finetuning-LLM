from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .groupBy("dept_name")
    .agg(F.max("budget").alias("max_budget"))
)

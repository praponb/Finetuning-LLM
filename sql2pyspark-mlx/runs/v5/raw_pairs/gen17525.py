from pyspark.sql import functions as F

result = (
    df_departments
    .agg(F.sum("budget").alias("total_budget"))
    .orderBy(F.col("total_budget").desc())
)

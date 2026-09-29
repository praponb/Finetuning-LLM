from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .orderBy(F.col("dept_name").desc())
)

from pyspark.sql import functions as F

result = (
    df_departments
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy("avg_budget")
)

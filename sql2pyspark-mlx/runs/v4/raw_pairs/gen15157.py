from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-11-21", "2025-05-01"))
    .groupBy(F.quarter("end_date").alias("end_quarter"), "status", "plan")
    .agg(F.count("*").alias("row_count"))
)

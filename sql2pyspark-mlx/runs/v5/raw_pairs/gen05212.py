from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(F.count("*").alias("row_count"))
)

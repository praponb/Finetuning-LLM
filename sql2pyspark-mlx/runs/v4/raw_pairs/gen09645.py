from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.quarter("end_date").alias("end_quarter"))
    .agg(F.count("*").alias("cnt"))
    .orderBy("status", F.col("end_quarter").desc())
)

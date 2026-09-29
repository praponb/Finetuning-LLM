from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 30) & ((F.col("end_date") < "2023-11-01") | (F.col("end_date") >= "2023-04-01")))
    .select("status", "start_date", F.quarter("end_date").alias("end_date_quarter"))
    .orderBy(F.col("start_date").desc())
)

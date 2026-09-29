from pyspark.sql import functions as F

max_start_date = df_subscriptions.filter(F.col("end_date") >= "2025-02-16").agg(F.max("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") == max_start_date)
    .select("id", "start_date", "plan")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.min("start_date").alias("min_start_date"))
    .orderBy("min_start_date")
)

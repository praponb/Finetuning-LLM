from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(F.min("end_date").alias("min_end_date"))
    .orderBy("min_end_date")
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("delivered_date").alias("event_date"))
    .subtract(df_subscriptions.select(F.col("start_date").alias("event_date")))
)

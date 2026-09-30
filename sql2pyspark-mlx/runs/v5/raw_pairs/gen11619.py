from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.col("last_restock").alias("event_date"))
    .intersect(df_subscriptions.select(F.col("end_date").alias("event_date")))
)

from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .intersect(df_subscriptions.select(F.col("start_date").alias("event_date")))
)

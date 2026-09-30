from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .subtract(df_subscriptions.select(F.col("end_date").alias("event_date")))
)

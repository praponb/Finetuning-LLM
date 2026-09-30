from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select(F.col("end_date").alias("event_date"))
    .intersect(df_store_sales.select(F.col("sale_date").alias("event_date")))
)

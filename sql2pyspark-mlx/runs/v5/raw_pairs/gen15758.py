from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"))
    .intersect(df_accounts.select(F.col("opened_date").alias("event_date")))
    .orderBy("event_date")
)

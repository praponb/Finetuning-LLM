from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "East")
    .select("opened_date")
    .subtract(df_subscriptions.select(F.col("start_date").alias("opened_date")))
)

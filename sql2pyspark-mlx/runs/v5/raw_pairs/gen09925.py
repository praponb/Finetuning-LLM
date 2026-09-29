from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%IAM%"))
    .select("opened_date")
    .unionByName(df_subscriptions.select(F.col("start_date").alias("opened_date")))
    .distinct()
)

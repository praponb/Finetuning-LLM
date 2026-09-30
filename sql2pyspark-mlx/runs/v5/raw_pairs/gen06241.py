from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select(F.col("start_date").alias("event_date"))
    )
)

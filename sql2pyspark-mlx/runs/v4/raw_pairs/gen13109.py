from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([2, 6, 8, 15]))
    .select("id", "customer_id", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("id", "customer_id", "end_date")
    )
    .distinct()
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .select("customer_id", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select("customer_id", "start_date")
    )
    .distinct()
)

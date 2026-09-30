from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "basic")
    .select("plan", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id") == 16)
        .select("plan", "customer_id")
    )
)

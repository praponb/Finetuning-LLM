from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select("plan", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isin([17, 33]))
        .select("plan", "status")
    )
)

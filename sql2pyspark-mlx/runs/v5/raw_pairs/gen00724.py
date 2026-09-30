from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("amount").alias("value"))
    .intersect(
        df_subscriptions
        .filter(F.col("status") != "ACTIVE")
        .select(F.col("monthly_fee").alias("value"))
    )
)

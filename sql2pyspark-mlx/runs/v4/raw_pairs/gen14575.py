from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"))
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") <= 220)
        .select(F.col("monthly_fee").alias("value"))
    )
)

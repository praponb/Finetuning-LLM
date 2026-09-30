from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("monthly_fee").alias("value"))
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").between(260, 320))
        .select(F.col("unit_price").alias("value"))
    )
)

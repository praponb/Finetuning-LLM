from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("kind"))
    .union(df_order_items.select("unit_price", F.lit("order_item").alias("kind")))
    .distinct()
)

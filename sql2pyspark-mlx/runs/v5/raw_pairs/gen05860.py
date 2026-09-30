from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2025)
    .select(F.lit("subscription").alias("source"), "monthly_fee")
    .union(
        df_orders
        .filter(F.year("order_date") == 2023)
        .select(F.lit("order").alias("source"), "amount")
    )
    .distinct()
)

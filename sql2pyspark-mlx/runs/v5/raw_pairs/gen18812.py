from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("amount") >= 1500)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.floor("shipping_fee").alias("shipping_fee_whole"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        "src.amount",
        "src.shipping_fee",
        "src.shipping_fee_whole",
        F.col("c.signup_date"),
        F.col("c.city"),
    )
)

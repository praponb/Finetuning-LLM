from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("discount") >= 0.22))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.lower("status").alias("status_lower"),
    )
)

result = (
    valid
    .select("channel", "status")
    .exceptAll(
        df_orders
        .filter(F.col("amount") < 1130)
        .select("channel", "status")
    )
)

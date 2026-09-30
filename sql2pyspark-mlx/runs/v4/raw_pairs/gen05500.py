from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-01-28").cast("date")) & (F.col("shipping_fee") <= 10))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.lower("status").alias("status_lower"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    base
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("status") == "RETURNED")
        .select("id")
    )
)

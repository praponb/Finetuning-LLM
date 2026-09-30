from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("store_id") == 24)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.ceil("amount").alias("amount_whole"),
    )
)

result = (
    cleaned
    .select("channel")
    .intersect(
        df_orders
        .filter(F.col("status") == "SHIPPED")
        .select("channel")
    )
)

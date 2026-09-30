from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "SHIPPED", "RETURNED"]))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.lower("channel").alias("channel_lower"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"), F.col("p.region"), F.col("p.opened_date"))
)

from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        F.last_day("order_date").alias("order_date_month_end"),
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("status", ["RETURNED", "SHIPPED"])
    .agg(F.max("amount_with_shipping"))
    .select(
        "customer_id",
        F.col("RETURNED").alias("returned"),
        F.col("SHIPPED").alias("shipped"),
    )
)

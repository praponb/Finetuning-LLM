from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.date_trunc("year", "order_date").alias("order_date_period"),
    )
)

result = (
    valid
    .groupBy("customer_id")
    .pivot("status", ["SHIPPED", "CANCELLED", "PENDING"])
    .agg(F.max("shipping_fee"))
)

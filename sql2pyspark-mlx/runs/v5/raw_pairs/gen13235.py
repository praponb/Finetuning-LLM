from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("shipping_fee") >= 26)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.least("shipping_fee", "amount").alias("least_shipping_fee_amount"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "channel"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("s.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.discount")), 2).alias("avg_discount"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)

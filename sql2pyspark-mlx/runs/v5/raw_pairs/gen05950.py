from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-07-23").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("shipping_fee") * F.col("amount")).alias("amount_with_shipping"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "channel"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("e.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.discount")), 2).alias("avg_discount"),
        F.min(F.col("e.shipping_fee")).alias("min_shipping_fee"),
        F.grouping_id().alias("level_id"),
    )
)

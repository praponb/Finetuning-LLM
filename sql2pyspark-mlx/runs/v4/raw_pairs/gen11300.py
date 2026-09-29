from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-08-31")
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("os.latest_delivered_date"),
        F.col("os.first_shipped_date"),
    )
    .orderBy(F.col("discount").desc())
)

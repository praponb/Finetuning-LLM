from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.order_id"),
        F.col("orders.shipping_fee"),
        F.col("orders.order_date"),
        (F.col("orders.amount") + F.col("orders.shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy(F.col("shipping_fee").desc())
)

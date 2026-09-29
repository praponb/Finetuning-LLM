from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.shipping_fee") >= 29) & F.col("o.discount").isNotNull())
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
        F.col("o.amount"),
        F.col("o.discount"),
    )
    .orderBy("delivered_date")
)

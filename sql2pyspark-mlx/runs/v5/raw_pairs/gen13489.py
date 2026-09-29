from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee").between(17, 37))
    .select(
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
    )
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.cost") > 59)
    .select(F.col("o.customer_id"), F.col("s.delivered_date"), F.col("s.shipped_date"))
)

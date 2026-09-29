from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount") >= 0.14)
    .select(F.col("s.cost"), F.col("s.delivered_date"), F.col("o.status"))
)

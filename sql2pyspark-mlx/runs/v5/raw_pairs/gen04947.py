from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(F.col("o.order_date"), F.col("s.status"), F.col("s.carrier"))
)

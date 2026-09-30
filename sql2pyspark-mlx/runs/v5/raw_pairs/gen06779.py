from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("s.carrier") == "UPS") | (F.col("o.amount") < 1550))
    .select(F.col("o.channel"), F.col("s.shipped_date"))
    .orderBy("shipped_date")
)

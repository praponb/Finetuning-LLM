from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .select(F.col("s.status"), F.col("o.order_date"))
    .orderBy(F.col("order_date").desc())
)

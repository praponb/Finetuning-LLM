from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.status").isin(["IN_TRANSIT", "LOST", "DELIVERED"]))
    .select(F.col("s.id"), F.col("s.cost"), F.col("o.store_id"))
    .orderBy(F.col("id").desc())
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(~F.col("region").isin(["East", "West", "South"])).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("o.status") == "CANCELLED")
    .select(F.col("o.id"), F.col("o.discount"), F.col("o.shipping_fee"))
)

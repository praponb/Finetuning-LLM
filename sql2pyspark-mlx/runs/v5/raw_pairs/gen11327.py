from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("region") == "North").alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("o.shipping_fee").between(28, 30))
    .select(F.col("o.id"), F.col("o.status"))
)

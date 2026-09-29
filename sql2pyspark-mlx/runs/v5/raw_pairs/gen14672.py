from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .groupBy(F.col("stores.region"))
    .agg(F.min(F.col("orders.shipping_fee")).alias("min_shipping_fee"))
    .orderBy("region")
)

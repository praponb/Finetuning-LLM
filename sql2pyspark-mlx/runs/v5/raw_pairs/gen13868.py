from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.filter(F.col("shipping_fee") > 21).alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_anti")
    .select(F.col("stores.id"), F.col("stores.city"), F.col("stores.store_name"))
)

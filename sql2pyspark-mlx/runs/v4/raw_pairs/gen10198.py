from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("stores.city"), F.col("stores.region"))
    .agg(F.sort_array(F.collect_set(F.col("orders.status"))).alias("status_list"))
)

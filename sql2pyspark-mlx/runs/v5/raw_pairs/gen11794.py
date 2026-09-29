from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .groupBy(F.col("stores.city"))
    .agg(F.sort_array(F.collect_set(F.col("payments.status"))).alias("status_list"))
    .orderBy("city")
)

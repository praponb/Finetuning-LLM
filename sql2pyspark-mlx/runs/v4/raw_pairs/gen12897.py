from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.shipping_fee") <= 26)
    .groupBy(F.col("customers.segment"))
    .agg(F.sort_array(F.collect_set(F.col("orders.status"))).alias("status_list"))
)

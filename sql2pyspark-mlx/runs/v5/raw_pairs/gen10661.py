from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(F.col("c.is_active"))
    .agg(F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"))
)

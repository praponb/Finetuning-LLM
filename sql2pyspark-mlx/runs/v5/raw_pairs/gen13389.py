from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("device")).alias("device_list"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.city"), F.col("c.country"), F.col("t.device_list"))
)

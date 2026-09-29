from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("device"))).alias("device_values"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("customer_stats.device_values"),
        F.col("customer_stats.browser_list"),
    )
)

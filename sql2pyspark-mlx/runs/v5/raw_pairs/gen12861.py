from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.countDistinct("device").alias("unique_devices"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .filter(F.col("customer_stats.unique_devices") > 3)
    .select(
        F.col("customers.id"),
        F.col("customers.age"),
        F.col("customer_stats.browser_list"),
        F.col("customer_stats.unique_devices"),
    )
)

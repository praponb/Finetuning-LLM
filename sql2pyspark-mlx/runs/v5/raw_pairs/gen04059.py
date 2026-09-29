from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .groupBy("customer_id")
    .agg(F.countDistinct("device").alias("unique_devices"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("CustomerStats"), F.col("customers.id") == F.col("CustomerStats.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("customers.age"),
        F.col("CustomerStats.unique_devices"),
    )
    .orderBy("id")
)

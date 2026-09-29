from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("country") == "JP")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("device").alias("unique_devices"),
        F.sum(F.when(F.col("device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.country"),
        F.col("customers.email"),
        F.col("t.unique_devices"),
        F.col("t.tablet_count"),
    )
)

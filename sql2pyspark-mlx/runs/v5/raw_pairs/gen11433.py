from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("latest_started_at"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.city"),
        F.col("customer_summary.latest_started_at"),
        F.col("customer_summary.device_list"),
    )
    .orderBy("id", F.col("latest_started_at").desc())
)

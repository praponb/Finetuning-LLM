from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge", "Safari"]))
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max("started_at").alias("latest_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("t.device_list"),
        F.col("t.latest_started_at"),
    )
    .orderBy("latest_started_at")
)

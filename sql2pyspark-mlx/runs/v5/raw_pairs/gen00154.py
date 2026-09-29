from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .groupBy(F.col("customers.city"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.country")))).alias("country_values"),
        F.min(F.col("sessions.started_at")).alias("min_started_at"),
    )
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "right")
    .groupBy(F.col("customers.is_active"), F.col("customers.country"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.browser")))).alias("browser_values"),
    )
    .orderBy(F.col("browser_values").desc())
)

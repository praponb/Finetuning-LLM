from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(~F.col("sessions.country").isin(["TH", "SG"]))
    .groupBy(F.col("customers.country"), F.col("customers.is_active"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.device")))).alias("device_values"),
        F.count(F.when(F.col("sessions.browser") == "Safari", 1)).alias("safari_count"),
    )
)

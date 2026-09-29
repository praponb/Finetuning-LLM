from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("customers.country") == "TH")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("customers.age"),
        F.col("customers.is_active"),
        F.col("customers.country"),
        F.concat_ws("/", F.col("sessions.device"), F.col("sessions.browser")).alias("device_browser_key"),
    )
)

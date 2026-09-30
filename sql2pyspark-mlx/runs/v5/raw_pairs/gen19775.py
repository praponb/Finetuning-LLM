from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter((F.col("customers.country") == "DE") & F.col("sessions.browser").isin(["Firefox", "Safari"]))
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
        F.col("customers.name"),
        F.col("customers.country"),
    )
    .orderBy(F.col("id").desc(), "started_at")
)

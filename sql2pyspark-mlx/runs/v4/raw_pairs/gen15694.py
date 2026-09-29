from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("sessions.country").isin(["UK", "US", "SG"]))
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
    )
    .orderBy(F.col("customer_id").desc())
)

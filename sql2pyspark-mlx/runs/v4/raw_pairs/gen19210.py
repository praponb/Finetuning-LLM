from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("sessions.customer_id").isNull())
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("customers.email"),
        F.col("customers.country"),
        F.col("customers.is_active"),
    )
    .orderBy(F.col("email").desc(), "id")
    .limit(1)
)

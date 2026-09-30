from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter((F.length(F.col("customers.name")) > 5) & (F.col("customers.credit_limit") < 12300))
    .select(
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("customers.email"),
        F.col("customers.is_active"),
    )
    .orderBy("browser")
)

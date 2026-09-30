from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .filter(~F.col("customers.email").like("%@gmail.com") & (F.length(F.col("customers.email")) < 8))
    .select(
        F.col("customers.is_active"),
        F.col("customers.name"),
        F.col("sessions.customer_id"),
    )
    .orderBy("name", F.col("customer_id").desc())
)

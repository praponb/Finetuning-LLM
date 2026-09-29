from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "outer")
    .filter(F.col("sessions.browser") != "Edge")
    .select(F.col("sessions.customer_id"), F.col("customers.is_active"))
    .orderBy("customer_id")
)

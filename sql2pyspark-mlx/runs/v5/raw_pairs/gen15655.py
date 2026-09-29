from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("browser") == "Chrome", 1).otherwise(0)).alias("chrome_count"),
    )
)

result = (
    df_customers.alias("customers")
    .join(sessions_by_customer.alias("sessions_by_customer"), F.col("customers.id") == F.col("sessions_by_customer.customer_id"), "inner")
    .filter(F.col("sessions_by_customer.chrome_count") > 63)
    .select(
        F.col("customers.id"),
        F.col("customers.name"),
        F.col("customers.email"),
        F.col("sessions_by_customer.chrome_count"),
    )
)

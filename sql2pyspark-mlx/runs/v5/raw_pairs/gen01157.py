from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.filter(F.col("started_at") < "2024-03-01").alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left_anti")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customers.email"),
        F.col("customers.age"),
    )
)

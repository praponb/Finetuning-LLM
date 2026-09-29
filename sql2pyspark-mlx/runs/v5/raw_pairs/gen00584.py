from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.year(F.col("customers.signup_date")).alias("signup_year"))
    .agg(
        F.count(F.when(F.col("sessions.country") == "SG", 1)).alias("sg_count"),
        F.min(F.col("sessions.started_at")).alias("first_started_at"),
    )
)

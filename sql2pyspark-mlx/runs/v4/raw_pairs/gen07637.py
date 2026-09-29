from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter((F.col("customers.signup_date") < "2025-07-01") | F.col("customers.city").isin(["New York", "Chiang Mai"]))
    .select(
        F.col("customers.city"),
        F.col("customers.age"),
        F.col("customers.signup_date"),
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
        F.col("sessions.browser"),
    )
)

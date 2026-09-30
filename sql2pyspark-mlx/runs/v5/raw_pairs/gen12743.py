from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("sessions.device").isin(["desktop", "mobile"]))
    .select(
        F.col("sessions.browser"),
        F.col("customers.signup_date"),
        F.col("customers.credit_limit"),
        F.col("customers.age"),
    )
)

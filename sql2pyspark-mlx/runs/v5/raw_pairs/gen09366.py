from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.signup_date").between("2023-11-01", "2025-10-13"))
    .select(
        F.col("web_events.id"),
        F.col("web_events.session_id"),
        F.col("web_events.page"),
        F.col("sessions.device"),
        F.col("sessions.browser"),
        F.col("customers.country"),
        F.col("customers.credit_limit"),
    )
)

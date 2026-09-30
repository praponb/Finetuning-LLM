from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isNull())
    .select(F.col("we.id"), F.col("we.page"), F.col("s.device"), F.col("c.signup_date"))
)

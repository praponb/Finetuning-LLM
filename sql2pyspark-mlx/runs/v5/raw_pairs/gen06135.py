from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~((F.col("we.page") == "/checkout") & (F.col("we.duration_sec") == 295)))
    .select(
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("c.credit_limit"),
    )
)

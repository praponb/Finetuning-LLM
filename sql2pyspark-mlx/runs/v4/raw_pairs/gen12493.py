from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~F.col("c.segment").isin(["Corporate", "SMB", "Consumer"]) & F.col("we.page").isin(["/home", "/product", "/search"]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.age"),
        F.col("c.city"),
    )
)

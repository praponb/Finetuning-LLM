from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(~(F.col("c.country").isin(["UK", "DE"]) | (F.to_date(F.col("s.started_at")) >= "2025-10-01")))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("c.segment"), F.col("c.email"))
)

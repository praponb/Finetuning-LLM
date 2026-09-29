from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.country").isin(["US", "SG"]) | (F.col("s.customer_id") == 10))
    .select(
        F.col("c.credit_limit"),
        F.col("c.segment"),
        F.col("c.country"),
        F.col("s.started_at"),
    )
    .orderBy(F.col("credit_limit").desc(), "started_at")
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("s.country").isin(["JP", "SG"]) & (F.to_date(F.col("s.started_at")) < "2023-08-01"))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("c.country"),
    )
    .orderBy("country", "id")
    .limit(50)
)

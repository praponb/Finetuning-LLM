from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isNull())
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.segment"),
    )
    .agg(F.min(F.col("s.started_at")).alias("min_started_at"))
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.min(F.col("s.started_at")).alias("min_started_at"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 2)
    .orderBy("min_started_at", "signup_month")
    .limit(50)
)

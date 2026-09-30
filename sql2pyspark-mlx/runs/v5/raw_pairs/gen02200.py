from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.length(F.col("customers.name")) < 11)
    .groupBy(
        F.when(F.col("customers.credit_limit") >= 15700, "high").when(F.col("customers.credit_limit") >= 4800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.max(F.col("sessions.started_at")).alias("last_started_at"),
        F.max(F.col("sessions.started_at")).alias("latest_started_at"),
    )
    .orderBy(F.col("latest_started_at").desc(), "credit_limit_bucket")
)

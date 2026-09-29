from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .groupBy("customer_id")
    .agg(F.max("started_at").alias("latest_started_at"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.name"), F.col("sbc.latest_started_at"))
    .orderBy(F.col("id").desc())
)

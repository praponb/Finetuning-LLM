from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.to_date("started_at") < "2025-03-01")
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .filter(F.col("sbc.desktop_count") > 4)
    .select(F.col("c.segment"), F.col("c.city"), F.col("sbc.desktop_count"))
    .orderBy("desktop_count", F.col("segment").desc())
)

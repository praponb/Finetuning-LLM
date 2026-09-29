from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") <= "2025-05-01")
    .groupBy(
        F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_month"),
        F.when(F.col("s.device").isin(["desktop", "tablet"]), "tier_1").otherwise("other").alias("device_group"),
    )
    .agg(F.count(F.when(F.col("we.page") == "/cart", 1)).alias("cart_count"))
    .orderBy("device_group", F.col("started_month").desc())
)

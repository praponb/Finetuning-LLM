from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.browser").isin(["Safari", "Edge"]))
    .select(F.col("c.email"), F.col("c.city"), F.col("s.device"), F.col("s.started_at"))
    .orderBy("city")
)

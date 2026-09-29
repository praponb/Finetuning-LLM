from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(F.count(F.col("s.id")).alias("session_count"))
    .orderBy("session_count", F.col("segment").desc())
)

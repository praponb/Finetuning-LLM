from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") >= 34)
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(F.count(F.col("s.id")).alias("session_count"))
)

from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id").isNotNull())
    .groupBy(F.col("c.id"), F.col("c.email"))
    .agg(F.count(F.col("s.id")).alias("session_count"))
    .filter(F.col("session_count") > 3)
)

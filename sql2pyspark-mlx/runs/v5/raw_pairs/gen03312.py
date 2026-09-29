from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.count(F.when(F.col("s.browser") == "Safari", 1)).alias("safari_count"),
    )
    .filter(F.col("safari_count") > 4)
)

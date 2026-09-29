from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.count(F.when(F.col("s.browser") == "Edge", 1)).alias("edge_count"),
    )
    .filter(F.col("session_count") >= 4)
    .orderBy("session_count")
)

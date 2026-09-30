from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.credit_limit").between(13200, 16100))
    .select(F.col("c.id"), F.col("c.segment"))
)

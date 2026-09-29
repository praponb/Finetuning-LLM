from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.length("email") >= 6).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.device"))
)

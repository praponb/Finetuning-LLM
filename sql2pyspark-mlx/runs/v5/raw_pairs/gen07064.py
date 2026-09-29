from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.segment").isNull())
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.name"), F.col("c.country"))
)

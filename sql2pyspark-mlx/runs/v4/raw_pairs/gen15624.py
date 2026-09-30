from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("credit_limit").isNotNull()).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.customer_id").isin([5, 18, 38]))
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy("country", F.col("id").desc())
)

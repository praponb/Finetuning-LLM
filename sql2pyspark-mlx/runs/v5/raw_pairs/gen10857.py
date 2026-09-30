from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit") >= 15500)
    .select(F.col("s.browser"), F.col("c.email"), F.col("c.city"), F.col("c.country"))
    .orderBy("country")
)

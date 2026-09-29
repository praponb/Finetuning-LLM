from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("credit_limit") < 7000).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.country") == "TH")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.device"), F.col("s.browser"))
)

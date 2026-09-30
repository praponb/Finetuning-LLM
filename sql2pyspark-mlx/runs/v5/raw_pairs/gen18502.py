from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(F.countDistinct(F.col("s.customer_id")).alias("unique_customers"))
    .orderBy(F.col("unique_customers").desc())
)

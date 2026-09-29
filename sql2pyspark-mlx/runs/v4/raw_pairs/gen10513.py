from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"), F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.count(F.when(F.col("s.browser") == "Firefox", 1)).alias("firefox_count"))
)

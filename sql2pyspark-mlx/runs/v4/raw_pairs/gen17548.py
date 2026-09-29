from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("customer_id").isin([1, 13, 26]))
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.firefox_count") >= 9)
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("cs.firefox_count"))
)

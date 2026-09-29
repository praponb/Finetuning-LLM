from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.city"), F.col("cs.firefox_count"))
)

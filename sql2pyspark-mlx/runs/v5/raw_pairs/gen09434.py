from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "tablet"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("browser") == "Firefox", 1).otherwise(0)).alias("firefox_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.city"), F.col("cs.firefox_count"))
)

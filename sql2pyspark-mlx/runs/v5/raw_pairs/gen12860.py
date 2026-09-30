from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("S"), F.col("c.id") == F.col("S.customer_id"), "inner")
    .filter(F.col("S.desktop_count") > 22)
    .select(F.col("c.id"), F.col("c.name"), F.col("S.desktop_count"))
    .orderBy("id")
    .limit(5)
)

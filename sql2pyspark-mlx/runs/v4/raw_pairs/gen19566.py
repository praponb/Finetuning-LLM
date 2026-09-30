from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.firefox_count") >= 4)
    .select(F.col("c.country"), F.col("ct.firefox_count"), F.col("ct.first_started_at"))
    .orderBy(F.col("first_started_at").desc())
)

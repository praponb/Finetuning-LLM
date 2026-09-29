from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.sum(F.when(F.col("browser") == "Chrome", 1).otherwise(0)).alias("chrome_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("cs.first_started_at"),
        F.col("cs.chrome_count"),
    )
    .orderBy(F.col("id").desc(), F.col("first_started_at").desc())
    .limit(10)
)

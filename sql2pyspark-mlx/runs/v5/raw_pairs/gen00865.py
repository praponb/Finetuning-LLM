from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_sessions"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.num_sessions") >= 6)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("cs.num_sessions"),
        F.col("cs.de_count"),
    )
)

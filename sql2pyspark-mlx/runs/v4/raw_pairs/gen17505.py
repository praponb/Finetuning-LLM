from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.hour("started_at").between(12, 15))
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("ct.total_count"),
        F.col("ct.mobile_count"),
    )
)

from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.month("end_date").isin([5, 7, 8]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.cancelled_count") >= 50)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("cs.cancelled_count"),
    )
    .orderBy("country")
)

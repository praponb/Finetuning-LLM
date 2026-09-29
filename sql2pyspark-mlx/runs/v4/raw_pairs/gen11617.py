from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.pending_count") > 57)
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("t.pending_count"),
        F.col("t.stddev_discount"),
    )
)

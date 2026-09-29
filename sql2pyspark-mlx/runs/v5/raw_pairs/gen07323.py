from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("t.avg_shipping_fee"),
        F.col("t.pending_amount"),
    )
    .orderBy("city")
)

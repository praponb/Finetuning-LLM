from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("t.pct_success"),
        F.col("t.min_amount"),
    )
    .orderBy("customer_id", F.col("pct_success").desc())
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("p.paid_at") < "2024-03-01")
    .groupBy(F.col("o.status"), F.col("o.store_id"))
    .agg(
        F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("status", F.col("store_id").desc())
    .limit(1)
)

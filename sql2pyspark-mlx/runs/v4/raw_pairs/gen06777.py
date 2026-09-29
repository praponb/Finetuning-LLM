from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.when(F.col("o.status").isin(["PENDING", "RETURNED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("p.status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNotNull())
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.sum(F.when(F.col("p.status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .filter(F.col("payment_count") > 5)
    .orderBy("avg_amount")
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.order_id") == 9)
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.sum(F.when(F.col("p.status") == "REFUNDED", F.col("p.amount")).otherwise(0)).alias("refunded_amount"),
    )
    .filter(F.col("payment_count") >= 5)
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.when(F.col("p.status") == "FAILED", 1)).alias("failed_count"),
        F.sum(F.when(F.col("p.method") == "promptpay", F.col("p.amount")).otherwise(0)).alias("promptpay_amount"),
    )
    .orderBy(F.col("promptpay_amount").desc())
)

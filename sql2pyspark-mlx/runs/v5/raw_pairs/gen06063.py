from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.store_id") == 22)
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.when(F.col("p.method") == "promptpay", 1)).alias("promptpay_count"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
)

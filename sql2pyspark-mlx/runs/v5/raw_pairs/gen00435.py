from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee") >= 21)
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("p.method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
    .orderBy(F.col("promptpay_count").desc())
)

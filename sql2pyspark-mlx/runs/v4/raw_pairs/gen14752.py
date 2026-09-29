from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") >= 14)
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.count(F.when(F.col("p.method") == "card", 1)).alias("card_count"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .orderBy("channel", "status")
    .limit(3)
)

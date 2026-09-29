from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.count(F.when(F.col("p.method") == "card", 1)).alias("card_count"),
    )
    .filter(F.col("max_amount") >= 570)
    .orderBy(F.col("card_count").desc(), "status")
)

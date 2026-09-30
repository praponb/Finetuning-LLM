from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") < 9431)
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .orderBy("channel")
)

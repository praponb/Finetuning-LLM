from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.round(((100 * F.sum(F.when(F.col("p.method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
    .orderBy("payment_count", "id", "channel")
    .limit(1)
)

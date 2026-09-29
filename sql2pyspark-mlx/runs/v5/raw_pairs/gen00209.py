from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("payment_count").desc(), "id")
    .limit(10)
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.channel").isin(["partner", "store", "mobile"]) | (F.col("p.paid_at") < "2025-06-01"))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.store_id"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("order_amount").desc())
)

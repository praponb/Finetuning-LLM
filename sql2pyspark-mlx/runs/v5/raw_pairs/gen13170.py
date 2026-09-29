from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & (F.col("p.status") != "FAILED"))
    .select(F.col("p.order_id"), F.col("o.amount"), F.col("o.discount"))
)

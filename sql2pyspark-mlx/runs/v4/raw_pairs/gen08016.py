from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.status") != "FAILED")
    .select(
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("p.paid_at"),
    )
    .orderBy(F.col("amount").desc(), "paid_at")
)

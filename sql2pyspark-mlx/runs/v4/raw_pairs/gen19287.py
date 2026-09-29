from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("p.amount"),
        F.col("p.status"),
    )
    .orderBy(F.col("status").desc())
)

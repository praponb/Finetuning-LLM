from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(F.col("p.id"), F.col("p.paid_at"), F.col("o.order_date"))
)

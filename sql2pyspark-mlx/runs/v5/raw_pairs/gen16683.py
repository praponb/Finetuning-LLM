from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .select(F.col("p.id"), F.col("p.amount"), F.col("o.discount"), F.col("o.status"))
    .orderBy(F.col("status").desc())
)

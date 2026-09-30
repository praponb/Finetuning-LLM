from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.order_id").isNull() & (F.col("o.channel") == "store"))
    .select(F.col("o.amount"), F.col("p.method"))
)

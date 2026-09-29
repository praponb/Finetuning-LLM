from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .filter(F.col("payments.amount") > 1400)
    .select(F.col("payments.order_id"), F.col("orders.status"), F.col("orders.channel"))
    .orderBy("channel")
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(F.count("*").alias("payment_count"))
    .orderBy("channel")
    .limit(1)
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.amount") < 610)
    .groupBy(F.col("o.store_id"))
    .agg(F.count(F.col("p.amount")).alias("amount_count"))
)

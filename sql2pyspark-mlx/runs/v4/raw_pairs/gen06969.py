from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(F.min(F.col("p.amount")).alias("min_amount"))
    .filter(F.col("min_amount") >= 1500)
)

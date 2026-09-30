from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_statuss"))
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_methods"))
    .filter(F.col("unique_methods") >= 1)
)

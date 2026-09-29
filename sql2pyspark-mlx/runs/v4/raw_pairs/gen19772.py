from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.countDistinct(F.col("p.amount")).alias("unique_orders"),
    )
)

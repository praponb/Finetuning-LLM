from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.year(F.col("o.order_date")) == 2024)
    .select(
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
)

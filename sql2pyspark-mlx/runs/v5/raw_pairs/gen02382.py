from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 39, "high").when(F.col("o.shipping_fee") >= 17, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
)

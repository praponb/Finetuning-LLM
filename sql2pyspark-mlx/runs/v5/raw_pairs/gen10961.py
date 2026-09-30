from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.when(F.col("p.amount") >= 1740, "High").when(F.col("p.amount") >= 250, "Medium").otherwise("Low").alias("amount_tier"),
    )
    .orderBy(F.col("amount_tier").desc())
)

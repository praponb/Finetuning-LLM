from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.order_id").isNull() & F.col("p.amount").between(950, 1510))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.when(F.col("p.status").isin(["SUCCESS", "REFUNDED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

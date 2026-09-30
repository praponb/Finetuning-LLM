from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount").between(0.28, 0.39))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.store_id"),
        F.col("o.amount"),
    )
    .orderBy(F.col("shipped_date").desc())
)

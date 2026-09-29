from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.store_id").isNull() & F.col("s.sqft").between(6054, 10801))
    .select(F.col("p.id"), F.col("p.status"), F.col("o.order_date"), F.col("s.sqft"))
    .orderBy(F.col("order_date").desc())
)

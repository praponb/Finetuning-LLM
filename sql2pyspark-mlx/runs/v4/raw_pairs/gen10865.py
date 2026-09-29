from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc())
)

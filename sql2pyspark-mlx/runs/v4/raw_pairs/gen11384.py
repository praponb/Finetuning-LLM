from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([1, 9, 12]) & F.month(F.col("o.order_date")).isin([9, 11, 12]))
    .select(F.col("s.region"), F.col("o.channel"), F.col("o.order_date"))
    .orderBy("order_date")
)

from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy("opened_month")
)

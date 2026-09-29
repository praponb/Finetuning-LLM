from pyspark.sql import functions as F

base = df_order_items.filter(F.col("unit_price").isNull())

order_id_summary = (
    base
    .groupBy("order_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = order_id_summary.orderBy("unit_price_range")

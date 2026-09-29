from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.month(F.col("orders.order_date")).alias("order_month"))
    .agg(F.sum(F.col("order_items.unit_price")).alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 1300)
)

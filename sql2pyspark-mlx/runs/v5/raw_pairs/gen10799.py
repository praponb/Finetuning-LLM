from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.month("order_date").isin([2, 7, 12])).alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_semi")
    .select(F.col("order_items.id"), F.col("order_items.unit_price"))
)

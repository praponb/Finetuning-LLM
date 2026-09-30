from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.order_date") >= "2024-07-01")
    .groupBy(F.col("orders.status"))
    .agg(
        F.sum(F.col("order_items.quantity")).alias("total_quantity"),
        F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"),
    )
    .orderBy("status")
    .limit(3)
)

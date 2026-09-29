from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.col("order_date") >= "2023-12-01").alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("order_items.product_id").isNotNull())
    .select(F.col("order_items.id"), F.col("order_items.unit_price"))
    .orderBy(F.col("id").desc())
    .limit(3)
)

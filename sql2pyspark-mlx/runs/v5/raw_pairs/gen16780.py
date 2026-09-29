from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("customer_id").isin([30, 39])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.unit_price").between(200, 280))
    .select(F.col("oi.id"), F.col("oi.unit_price"), F.col("oi.product_id"))
    .orderBy(F.col("id").desc())
    .limit(10)
)

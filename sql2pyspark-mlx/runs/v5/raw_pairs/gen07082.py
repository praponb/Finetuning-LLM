from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.unit_price") < 280)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("id").desc())
)

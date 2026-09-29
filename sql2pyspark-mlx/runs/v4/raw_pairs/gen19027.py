from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("order_items.product_id") == 12)
    .groupBy(F.col("products.supplier_id"))
    .agg(F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"))
)

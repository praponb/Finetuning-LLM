from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("products.category"), F.col("products.brand"))
    .agg(F.sum(F.col("order_items.unit_price")).alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 600)
    .orderBy(F.col("total_unit_price").desc())
)

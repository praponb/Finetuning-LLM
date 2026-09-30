from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost").between(370, 380))
    .groupBy(F.col("p.brand"))
    .agg(F.min(F.col("oi.unit_price")).alias("min_unit_price"))
    .filter(F.col("min_unit_price") >= 310)
)

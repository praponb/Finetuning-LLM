from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.order_id").isin([4, 14, 22, 38]))
    .groupBy(F.col("p.brand"), F.col("p.supplier_id"))
    .agg(
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
    .orderBy(F.col("supplier_id").desc(), "brand")
    .limit(20)
)

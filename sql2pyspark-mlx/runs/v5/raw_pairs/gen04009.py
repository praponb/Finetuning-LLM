from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
    )
    .orderBy(F.col("brand").desc())
)

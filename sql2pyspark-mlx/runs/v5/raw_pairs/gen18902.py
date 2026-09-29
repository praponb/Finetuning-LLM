from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "SG")
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.min(F.col("oi.unit_price")).alias("min_unit_price"))
    .filter(F.col("min_unit_price") >= 330)
    .orderBy(F.col("launch_year").desc(), F.col("min_unit_price").desc())
)

from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("p.cost").between(270, 560))
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
    .orderBy(F.col("city").desc())
)

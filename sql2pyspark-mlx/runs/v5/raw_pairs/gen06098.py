from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.units") > 30)
    .groupBy(F.col("p.brand"))
    .agg(F.max(F.col("ss.revenue")).alias("max_revenue"))
    .orderBy(F.col("max_revenue").desc())
)

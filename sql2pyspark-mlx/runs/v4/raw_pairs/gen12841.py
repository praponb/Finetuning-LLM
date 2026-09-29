from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Berlin")
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(F.min(F.col("ss.revenue")).alias("min_revenue"))
    .filter(F.col("min_revenue") >= 1300)
)

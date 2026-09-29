from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("p.category").isin(["Sports", "Electronics", "Home"]))
    .groupBy(F.col("s.country"), F.col("s.city"))
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
    .orderBy(F.col("city").desc())
)

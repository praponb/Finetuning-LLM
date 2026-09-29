from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") >= 2.8)
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("s.country"),
    )
    .orderBy("country", "id")
    .limit(10)
)

from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.brand") == "Acme")
    .select(
        F.col("ss.sale_date"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.lower(F.col("s.supplier_name")).alias("supplier_name_lower"),
    )
)

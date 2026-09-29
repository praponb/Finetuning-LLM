from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("ss.revenue") > 3300) & F.col("s.country").isin(["JP", "DE"]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.initcap(F.trim(F.col("s.supplier_name"))).alias("supplier_name_title"),
    )
)

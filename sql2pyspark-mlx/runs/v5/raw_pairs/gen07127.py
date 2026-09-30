from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["US", "DE"]))
    .select(
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)

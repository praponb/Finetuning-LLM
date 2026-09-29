from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.supplier_id").isin([2, 21]) & (F.col("ss.units") != 37))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.rating"),
    )
)

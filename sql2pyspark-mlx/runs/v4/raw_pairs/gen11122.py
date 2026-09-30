from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.round(F.col("ss.revenue"), 2).alias("revenue_rounded"),
    )
    .orderBy(F.col("revenue_rounded").desc())
)

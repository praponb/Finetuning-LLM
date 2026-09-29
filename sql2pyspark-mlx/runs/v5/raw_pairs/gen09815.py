from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("ss.product_id").isin([2, 3, 4, 7]) & F.col("ss.revenue").between(1100, 3900))
    .select(
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.city"),
    )
    .orderBy("launch_date")
)

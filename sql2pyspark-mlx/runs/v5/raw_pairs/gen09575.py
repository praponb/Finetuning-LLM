from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").like("Run%"))
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
)

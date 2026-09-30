from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "bestseller"))
    .select(
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.rating"),
        F.col("s.city"),
    )
)

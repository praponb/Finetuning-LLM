from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("s.city"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
)

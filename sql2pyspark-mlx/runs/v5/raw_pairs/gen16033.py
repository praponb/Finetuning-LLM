from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter((F.col("p.supplier_id") == 30) & (F.col("s.country") == "UK"))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .select(
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)

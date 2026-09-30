from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "US")
    .select(
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
)

from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)

from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter((F.col("ss.revenue") <= 1800) & (F.col("ss.store_id") == 2))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("p.cost"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy("country", F.col("cost").desc())
)

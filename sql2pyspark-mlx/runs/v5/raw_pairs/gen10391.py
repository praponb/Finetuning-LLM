from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("s.supplier_name"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.upper(F.col("s.city")).alias("city_upper"),
    )
)

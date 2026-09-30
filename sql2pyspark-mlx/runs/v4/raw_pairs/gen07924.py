from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("s.country"),
        F.concat_ws(",", F.col("s.supplier_name"), F.col("s.city")).alias("supplier_name_city_key"),
    )
    .orderBy(F.col("stock_qty").desc())
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.rating"),
        F.concat_ws("/", F.col("s.supplier_name"), F.col("s.country")).alias("supplier_name_country_key"),
    )
    .orderBy("category", F.col("id").desc())
)

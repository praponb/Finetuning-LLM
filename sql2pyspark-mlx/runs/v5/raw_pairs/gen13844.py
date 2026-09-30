from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.rating"), F.col("s.city"))
)

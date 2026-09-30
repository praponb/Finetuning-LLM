from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.country"), F.col("b.supplier_name"))
)

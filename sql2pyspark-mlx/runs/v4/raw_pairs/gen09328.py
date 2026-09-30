from pyspark.sql import functions as F

result = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.country"), F.col("s.city"), F.col("s.supplier_name"))
)

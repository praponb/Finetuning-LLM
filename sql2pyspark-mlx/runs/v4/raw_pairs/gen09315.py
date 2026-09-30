from pyspark.sql import functions as F

result = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.rating"), F.col("d.supplier_name"), F.col("d.city"))
)

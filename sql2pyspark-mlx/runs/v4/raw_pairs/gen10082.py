from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.country") == "JP")
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy("id")
    .limit(5)
)

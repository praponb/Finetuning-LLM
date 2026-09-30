from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("cost") >= 190).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.country") == "DE")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"))
)

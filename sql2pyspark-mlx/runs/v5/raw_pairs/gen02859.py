from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isNull()).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.supplier_name").like("%Pacific%"))
    .select(F.col("s.id"), F.col("s.rating"))
)

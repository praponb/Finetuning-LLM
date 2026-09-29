from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("product_name").like("%Offic%")).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.city").isin(["Berlin", "London"]))
    .select(F.col("s.id"), F.col("s.city"), F.col("s.country"), F.col("s.rating"))
    .orderBy("city")
)

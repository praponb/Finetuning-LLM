from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["TH", "SG"]))
    .select(F.col("p.cost"), F.col("s.rating"), F.col("s.city"), F.col("s.country"))
    .orderBy(F.col("rating").desc())
)

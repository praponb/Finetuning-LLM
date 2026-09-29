from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(~(F.month(F.col("p.launch_date")).isin([2, 10, 11]) & (F.col("s.country") != "TH")))
    .select(F.col("s.rating"), F.col("s.country"), F.col("p.brand"))
    .orderBy(F.col("rating").desc())
)

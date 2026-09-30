from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("country").isin(["DE", "JP"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.supplier_id").isNull())
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.category"))
)

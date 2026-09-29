from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(F.col("s.rating"), F.col("s.country"), F.col("p.cost"), F.col("p.price"))
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost") < 640)
    .select(F.col("p.cost"), F.col("s.city"), F.col("s.rating"), F.col("s.country"))
)

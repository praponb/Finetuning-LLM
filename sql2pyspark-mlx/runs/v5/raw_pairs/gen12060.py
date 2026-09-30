from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.category") != "Fashion") & (F.col("s.rating") > 2.7))
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("s.rating"))
)

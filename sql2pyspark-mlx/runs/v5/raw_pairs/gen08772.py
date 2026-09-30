from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.upper(F.trim(F.col("s.supplier_name"))) == "PACIFIC WHOLESALE")
    .select(F.col("s.city"), F.col("p.price"), F.col("p.launch_date"))
    .orderBy(F.col("price").desc())
)

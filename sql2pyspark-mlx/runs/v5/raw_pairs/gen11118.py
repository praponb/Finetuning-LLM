from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.year("launch_date") == 2023).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.country") != "JP")
    .select(F.col("s.id"), F.col("s.supplier_name"))
    .orderBy(F.col("id").desc())
)

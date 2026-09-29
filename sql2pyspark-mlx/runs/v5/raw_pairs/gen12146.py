from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Pac%") & (F.col("city") == "Chiang Mai"))
    .select("supplier_name", "city")
)

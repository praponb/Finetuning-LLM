from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%City%") & (F.length("supplier_name") >= 5))
    .select("id", "country", "city")
    .orderBy(F.col("country").desc())
)

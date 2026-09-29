from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("city", "country")
    .distinct()
    .orderBy("city", F.col("country").desc())
)

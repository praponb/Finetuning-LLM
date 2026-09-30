from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .select("city")
    .distinct()
    .orderBy(F.col("city").desc())
    .limit(5)
)

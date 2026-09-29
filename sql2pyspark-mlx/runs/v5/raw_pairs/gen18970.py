from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .select("city")
    .distinct()
    .orderBy("city")
    .limit(5)
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "US")
    .select("id", "city")
    .orderBy("city", "id")
    .limit(1)
)

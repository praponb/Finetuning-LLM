from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Chiang Mai")
    .select("city", "country", "rating")
)

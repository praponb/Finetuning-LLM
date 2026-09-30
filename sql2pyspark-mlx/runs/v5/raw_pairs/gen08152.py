from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select("city", "country", "rating")
)

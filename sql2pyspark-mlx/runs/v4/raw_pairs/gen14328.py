from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.5)
    .select("country", "city", "rating", F.lower("country").alias("country_lower"))
)

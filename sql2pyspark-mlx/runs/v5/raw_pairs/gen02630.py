from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "city", "country", "rating", F.trim("country").alias("clean_country"))
)

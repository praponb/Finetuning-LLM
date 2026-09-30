from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "rating", F.trim("country").alias("clean_country"))
    .orderBy(F.col("rating").desc())
)

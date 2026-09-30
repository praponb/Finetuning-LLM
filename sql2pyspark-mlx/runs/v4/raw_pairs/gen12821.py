from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "rating", F.trim("city").alias("clean_city"))
    .orderBy(F.col("rating").desc())
)

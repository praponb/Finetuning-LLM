from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-10-22", "2024-12-15"))
    .select("id", "region", "sqft", "city", F.trim("city").alias("clean_city"))
)

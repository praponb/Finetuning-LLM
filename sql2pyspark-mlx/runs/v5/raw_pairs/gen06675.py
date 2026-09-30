from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.7)
    .select("country", "supplier_name", "rating", F.lower("city").alias("city_lower"))
    .orderBy(F.col("city_lower").desc(), "country")
)

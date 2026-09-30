from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment", F.lower("country").alias("country_lower"))
    .orderBy("segment")
)

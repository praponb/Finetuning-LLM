from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("age") != 44))
    .select("is_active", F.concat("city", F.lit("/"), "country").alias("city_country"))
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") == 2024)
    .select("id", "is_active", F.concat("city", F.lit(", "), "name").alias("city_name"))
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5900, 14100))
    .select("id", "name", F.concat_ws(",", "name", "city").alias("name_city_key"))
    .orderBy("name_city_key")
)

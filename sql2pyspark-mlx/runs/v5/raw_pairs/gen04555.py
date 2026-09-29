from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "age", F.concat_ws(",", "country", "name").alias("country_name_key"))
)

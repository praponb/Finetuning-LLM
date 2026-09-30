from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") == 48) | F.col("name").isNotNull())
    .select("id", "segment", F.concat("city", F.lit("/"), "email").alias("city_email"))
)

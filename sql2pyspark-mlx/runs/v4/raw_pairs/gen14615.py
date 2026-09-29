from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 7200)
    .withColumn("city_prefix", F.substring("city", 1, 3))
    .orderBy("email")
)

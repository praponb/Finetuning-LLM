from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 33)
    .select("id", "age", "country", F.upper("country").alias("country_upper"))
    .orderBy(F.col("age").desc(), "id")
    .limit(20)
)

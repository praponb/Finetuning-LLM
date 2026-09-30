from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 4)
    .select("segment", "signup_year", "cnt")
    .orderBy(F.col("signup_year").desc(), "segment")
    .limit(3)
)

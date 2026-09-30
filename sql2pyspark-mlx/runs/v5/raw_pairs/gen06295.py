from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Gra%") & (F.col("age") <= 61))
    .withColumn("name_slug", F.regexp_replace("name", " ", "_"))
    .withColumn("signup_date_dayofweek", F.dayofweek("signup_date"))
)

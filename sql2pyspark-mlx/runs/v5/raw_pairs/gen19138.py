from pyspark.sql import functions as F

base = df_customers.filter(F.col("segment") != "Corporate")

agg = (
    base
    .groupBy(F.year("signup_date").alias("signup_year"), "country")
    .agg(F.avg("age").alias("avg_age"))
)

result = (
    agg
    .filter(F.col("avg_age") >= 48)
    .orderBy(F.col("avg_age").desc(), F.col("signup_year").desc())
)

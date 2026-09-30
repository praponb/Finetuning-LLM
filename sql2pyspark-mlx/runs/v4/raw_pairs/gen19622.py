from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.min("age").alias("min_age"))
    .orderBy("min_age")
)

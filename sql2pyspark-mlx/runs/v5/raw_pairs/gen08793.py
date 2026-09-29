from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & F.col("email").isNotNull())
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.countDistinct("credit_limit").alias("unique_citys"))
)

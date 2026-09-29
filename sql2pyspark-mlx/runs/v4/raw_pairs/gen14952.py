from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.countDistinct("credit_limit").alias("unique_countrys"))
    .filter(F.col("unique_countrys") >= 5)
    .orderBy(F.col("unique_countrys").desc(), F.col("signup_year").desc())
    .limit(3)
)

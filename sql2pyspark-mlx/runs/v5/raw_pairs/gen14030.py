from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2024-05-01")
    .groupBy("is_active")
    .agg(F.countDistinct("credit_limit").alias("unique_citys"))
    .filter(F.col("unique_citys") < 2)
    .orderBy(F.col("unique_citys").desc())
)

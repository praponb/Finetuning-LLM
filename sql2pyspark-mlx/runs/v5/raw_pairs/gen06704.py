from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.countDistinct("age").alias("unique_citys"))
)

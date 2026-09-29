from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co") & (F.col("age") != 40) & (F.col("country") == "SG"))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.countDistinct("age").alias("unique_citys"))
    .filter(F.col("unique_citys") >= 1)
    .orderBy("signup_month", "unique_citys")
)

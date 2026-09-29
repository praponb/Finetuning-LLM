from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.countDistinct("age").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") >= 1)
    .orderBy("signup_month")
)

from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", F.month("signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("credit_limit")).otherwise(0)).alias("th_credit_limit"),
    )
    .filter(F.col("th_credit_limit") >= 36)
    .orderBy(F.col("signup_month").desc(), "city")
)

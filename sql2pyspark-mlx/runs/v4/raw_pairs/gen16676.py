from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(~F.col("email").like("%@outlook.com"))
    .withColumn("name_lower", F.lower("name"))
)

agg = (
    valid_customers
    .groupBy("city", "country")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
    )
)

result = (
    agg
    .filter(F.col("corporate_credit_limit") < 61)
    .select("city", "country", "min_signup_date", "corporate_credit_limit")
    .orderBy("city", "country")
)

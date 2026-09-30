from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("city") == "Bangkok") & F.col("city").isin(["Tokyo", "Chiang Mai", "Singapore"])) | F.col("email").like("%@gmail.com"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
    )
    .filter(F.col("corporate_count") < 3)
    .orderBy("corporate_count")
)

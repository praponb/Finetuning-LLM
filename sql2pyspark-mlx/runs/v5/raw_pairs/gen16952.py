from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"), "country")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.min("signup_date").alias("first_signup_date"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy("country")
)

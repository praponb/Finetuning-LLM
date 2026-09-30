from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("city") != "Tokyo"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.max("age").alias("max_age"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") < 1)
    .drop("age_count")
)

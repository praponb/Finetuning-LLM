from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") >= "2025-08-01") & (F.col("segment") == "SMB"))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum("age").alias("total_age"),
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
    .orderBy("total_age", "signup_month")
)

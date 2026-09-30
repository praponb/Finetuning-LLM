from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "US", "TH"]))
    .groupBy(
        F.when(F.col("age") >= 65, "high").when(F.col("age") >= 36, "mid").otherwise("low").alias("age_bucket"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .filter(F.col("age_range") < 23)
    .select("age_bucket", "signup_year", "age_range")
)

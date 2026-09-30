from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "JP"]))
    .groupBy(F.year("signup_date").alias("signup_year"), "country")
    .agg(
        F.min("age").alias("min_age"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .groupBy("country", F.month("signup_date").alias("signup_month"))
    .agg(
        F.max("age").alias("max_age"),
        F.max("signup_date").alias("last_signup_date"),
    )
)

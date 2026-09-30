from pyspark.sql import functions as F

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

result = (
    eligible
    .withColumn("year_start", F.date_trunc("YEAR", "signup_date"))
    .groupingSets([["year_start", "city"], ["year_start"]], "year_start", "city")
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
    )
)

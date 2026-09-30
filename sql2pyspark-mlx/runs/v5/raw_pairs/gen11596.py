from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") > F.to_date(F.lit("2023-06-12"))))
    .withColumn("signup_date_dayofweek", F.dayofweek("signup_date"))
    .withColumn("age_minus_credit_limit", (F.col("age") - F.col("credit_limit")))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

result = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < F.col("s.signup_date")), "left_anti")
)

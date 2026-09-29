from pyspark.sql import functions as F

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.browser") == "Safari"), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age"))).alias("corporate_total_age"),
        F.sum(F.when(F.col("country") == "UK", F.col("age")).otherwise(0)).alias("uk_age"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("corporate_total_age") < 1000000)
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.count("*").alias("num_sessions"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
    )
    .orderBy("num_sessions")
)

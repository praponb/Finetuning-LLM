from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
    )
)

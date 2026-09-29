from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("s.country") == "UK")
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.year(F.col("c.signup_date")).alias("signup_year"),
    )
    .agg(F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"))
    .orderBy(F.col("signup_year").desc(), "signup_quarter")
    .limit(50)
)

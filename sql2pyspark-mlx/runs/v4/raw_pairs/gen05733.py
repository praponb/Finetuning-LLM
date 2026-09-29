from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.hour(F.col("s.started_at")).between(8, 12))
    .groupBy(
        F.when(F.col("c.age") >= 61, "high").when(F.col("c.age") >= 38, "mid").otherwise("low").alias("age_bucket"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
        F.countDistinct(F.col("s.customer_id")).alias("unique_customers"),
    )
    .orderBy(F.col("signup_month").desc())
)

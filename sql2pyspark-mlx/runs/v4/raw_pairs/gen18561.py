from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isin([35, 38]))
    .groupBy(
        F.month(F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.sort_array(F.collect_set(F.col("s.country"))).alias("country_list"))
    .orderBy(F.col("signup_month").desc())
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(F.countDistinct(F.col("s.country")).alias("unique_countrys"))
    .filter(F.col("unique_countrys") > 2)
    .orderBy(F.col("signup_month").desc())
    .limit(5)
)

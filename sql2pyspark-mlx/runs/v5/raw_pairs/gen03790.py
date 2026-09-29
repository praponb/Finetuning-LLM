from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.age") == 47)
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.count(F.col("we.duration_sec")).alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") >= 1)
    .orderBy("duration_sec_count", F.col("signup_quarter").desc())
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.device").isin(["mobile", "tablet"]))
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        F.count(F.when(F.col("sessions.country") == "DE", 1)).alias("de_count"),
        F.count("*").alias("session_count"),
    )
    .filter(F.col("de_count") > 4)
    .orderBy(F.col("de_count").desc())
)

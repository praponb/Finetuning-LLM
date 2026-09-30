from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.year(F.col("customers.signup_date")).alias("signup_year"),
        F.when(F.col("customers.country").isin(["JP", "DE"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.count(F.when(F.col("sessions.device") == "mobile", 1)).alias("mobile_count"),
        F.max(F.col("sessions.started_at")).alias("latest_started_at"),
    )
    .filter(F.col("mobile_count") > 3)
)

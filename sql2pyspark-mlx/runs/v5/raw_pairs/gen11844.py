from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.country") == "SG")
    .groupBy(F.col("customers.country"))
    .agg(
        F.sum(F.when(F.col("sessions.country") == "US", 1).otherwise(0)).alias("us_count"),
        F.countDistinct(F.col("sessions.device")).alias("unique_devices"),
    )
    .filter(F.col("unique_devices") > 4)
    .orderBy(F.col("us_count").desc())
)

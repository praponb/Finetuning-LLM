from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("s.device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
    .orderBy(F.col("signup_month").desc())
)

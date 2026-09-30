from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") > 11200)
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("s.device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .orderBy("segment")
)

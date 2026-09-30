from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id").isin([11, 14]))
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
        F.sum(F.when(F.col("s.device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .orderBy("mobile_count")
)

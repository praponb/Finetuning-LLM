from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isNotNull())
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.segment"),
    )
    .agg(
        F.sum(F.when(F.col("s.device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
    )
)

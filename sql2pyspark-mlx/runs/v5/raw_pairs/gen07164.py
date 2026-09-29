from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") < 54)
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14900, "high").when(F.col("c.credit_limit") >= 6400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.device")))).alias("device_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.country")))).alias("country_values"),
    )
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("%Gar%"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.when(F.col("s.browser") == "Chrome", 1)).alias("chrome_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.device")))).alias("device_values"),
    )
    .orderBy(F.col("device_values").desc())
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.device")))).alias("device_values"),
    )
    .orderBy("device_values", "segment")
    .limit(1)
)

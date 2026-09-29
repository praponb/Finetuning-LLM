from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(F.col("s.country").isin(["TH", "UK", "JP"]))
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.country")))).alias("country_values"),
    )
)

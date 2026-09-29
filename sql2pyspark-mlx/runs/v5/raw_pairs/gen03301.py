from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.city"),
        F.col("c.age"),
        F.col("t.last_started_at"),
        F.col("t.browser_values"),
    )
)

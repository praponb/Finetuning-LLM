from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.email"), F.col("cs.country_values"), F.col("cs.firefox_count"))
    .orderBy(F.col("firefox_count").desc())
)

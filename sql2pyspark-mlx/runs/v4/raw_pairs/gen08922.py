from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "tablet"]))
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.country"),
        F.col("c.email"),
        F.col("cs.last_started_at"),
        F.col("cs.country_list"),
    )
    .orderBy("email")
)

from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.cnt") >= 8)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.col("t.channel_values"),
        F.col("t.cnt"),
    )
    .orderBy("channel_values")
)

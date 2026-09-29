from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("ct.country_values"),
    )
)

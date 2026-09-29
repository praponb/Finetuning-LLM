from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("shipping_fee") >= 39)
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.email"), F.col("c.age"), F.col("t.channel_values"))
    .orderBy("channel_values")
)

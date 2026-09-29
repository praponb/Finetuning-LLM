from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .orderBy("carrier_values")
)

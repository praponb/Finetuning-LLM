from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.status").isin(["RETURNED", "CANCELLED", "PENDING"]))
    .groupBy(F.col("o.customer_id"), F.col("o.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
        F.count(F.when(F.col("s.carrier") == "UPS", 1)).alias("ups_count"),
    )
)

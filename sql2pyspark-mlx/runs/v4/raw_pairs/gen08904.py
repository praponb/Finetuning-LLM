from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .groupBy(F.col("customers.country"))
    .agg(
        F.min(F.col("shipments.delivered_date")).alias("min_delivered_date"),
        F.sort_array(F.collect_set(F.col("shipments.carrier"))).alias("carrier_list"),
    )
    .orderBy("country", "min_delivered_date")
    .limit(5)
)

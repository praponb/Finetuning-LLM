from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.status") != "DELIVERED")
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"))
    .agg(
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
    )
    .filter(F.col("in_transit_count") > 61)
)

from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
        F.sum(F.when(F.col("s.status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
    .orderBy("carrier_values")
)

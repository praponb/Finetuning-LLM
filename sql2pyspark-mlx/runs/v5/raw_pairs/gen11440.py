from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.cost") >= 27)
    .groupBy(F.col("o.status"))
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
    )
    .orderBy(F.col("carrier_values").desc(), "status")
    .limit(10)
)

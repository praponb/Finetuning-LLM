from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.order_id") == 36)
    .groupBy(F.col("o.channel"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
    )
    .orderBy("channel")
)

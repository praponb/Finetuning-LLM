from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"), F.col("o.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
)

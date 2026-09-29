from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.channel").isNotNull())
    .groupBy(
        F.col("o.status"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
    )
    .orderBy("status")
)

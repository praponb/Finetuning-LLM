from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("t.max_cost"),
        F.col("t.carrier_list"),
    )
    .orderBy("max_cost")
)

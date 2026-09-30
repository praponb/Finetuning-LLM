from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 43, "high").when(F.col("o.shipping_fee") >= 7, "mid").otherwise("low").alias("shipping_fee_bucket"),
        F.when(F.col("o.amount") >= 1630, "high").when(F.col("o.amount") >= 780, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
    .orderBy("amount_bucket")
)

from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").isNotNull())
    .groupBy(
        F.when(F.col("o.discount") >= 0.34, "high").when(F.col("o.discount") >= 0.15, "mid").otherwise("low").alias("discount_bucket"),
        F.year(F.col("o.order_date")).alias("order_year"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
    .orderBy(F.col("discount_bucket").desc())
)

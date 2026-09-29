from pyspark.sql import functions as F

enriched = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.cost",
        "s.status",
        F.col("d.status").alias("order_status"),
        F.col("d.amount"),
        F.col("d.shipping_fee"),
    )
)

result = (
    enriched
    .groupBy(
        "order_id",
        F.when(F.col("shipping_fee") >= 35, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
    )
    .orderBy(F.col("order_id").asc_nulls_first())
)

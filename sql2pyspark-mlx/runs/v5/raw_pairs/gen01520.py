from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("cost_times_days_delivered_date_to_shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("cost_times_days_delivered_date_to_shipped_date", "id")
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("shipping_fee").desc(), "id")
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_times_days_delivered_date_to_shipped_date").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") > F.lit("2023-08-06").cast("date")))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

valid = (
    cleaned
    .filter(~F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        "carrier_upper",
        "resolved_carrier",
        (F.col("cost") * F.col("days_delivered_date_to_shipped_date")).alias("cost_times_days_delivered_date_to_shipped_date"),
        F.weekofyear("shipped_date").alias("shipped_date_weekofyear"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.sum("cost_times_days_delivered_date_to_shipped_date").over(w).alias("running_sum_cost_times_days_delivered_date_to_shipped_date"),
        F.count("cost_times_days_delivered_date_to_shipped_date").over(w).alias("running_count_cost_times_days_delivered_date_to_shipped_date"),
        F.avg("cost_times_days_delivered_date_to_shipped_date").over(w).alias("running_avg_cost_times_days_delivered_date_to_shipped_date"),
        F.lead("shipped_date_weekofyear", 1, 0).over(w2).alias("next_shipped_date_weekofyear"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.status",
        "s.carrier_upper",
        "s.cost_times_days_delivered_date_to_shipped_date",
        F.col("p.shipping_fee"),
    )
)

enriched_shipments = (
    joined
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("cost_times_days_delivered_date_to_shipped_date_bucket", F.ntile(5).over(w4))
)

result = (
    enriched_shipments
    .withColumn("cost_times_days_delivered_date_to_shipped_date_tier", F.when(F.col("cost_times_days_delivered_date_to_shipped_date") >= 90, "HIGH").otherwise("LOW"))
    .rollup("cost_times_days_delivered_date_to_shipped_date_tier", "status")
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
        F.count_if(F.col("status") == "IN_TRANSIT").alias("in_transit_events"),
        F.grouping("cost_times_days_delivered_date_to_shipped_date_tier").alias("gid"),
    )
)

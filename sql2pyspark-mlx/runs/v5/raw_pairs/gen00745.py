from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "order_status")
w2 = Window.partitionBy("order_status_2", "order_id").orderBy("order_date", "id")
w3 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status", "order_status_2")
w_running = Window.partitionBy("order_status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .select(
        "*",
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.status",
        "s.resolved_carrier",
        F.col("p.status").alias("order_status"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.status",
        "src.resolved_carrier",
        "src.order_status",
        F.col("o.status").alias("order_status_2"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "order_id",
        "carrier",
        "resolved_carrier",
        "shipping_fee",
        F.min("shipping_fee").over(w_ordered).alias("status_min_shipping_fee"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("shipping_fee").over(w3).alias("running_shipping_fee"),
        F.avg("shipping_fee").over(w_recent).alias("status_avg_shipping_fee"),
        F.sum("shipping_fee").over(w_running).alias("cumulative_shipping_fee"),
    )
)

result = (
    scored
    .groupBy("carrier")
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.max(F.when(F.col("carrier") == "FedEx", F.col("status_avg_shipping_fee"))).alias("fedex_max_status_avg_shipping_fee"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_rows"),
        F.min("event_seq").alias("min_event_seq"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy(F.col("ups_rows").desc_nulls_last())
)

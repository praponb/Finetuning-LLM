from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

valid = (
    df_shipments
    .filter(F.col("cost") < 70)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.date_sub("shipped_date", 67).alias("shipped_date_minus_67d"),
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)

with_windows = (
    valid
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        F.count("cost").over(w).alias("running_count_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.sum("cost").over(w).alias("running_cost"),
    )
)

matched_keys = (
    with_windows
    .select("id", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") < "2024-08-01")
        .select("id", "order_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.status"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.cost",
        "s.running_count_cost",
        "s.running_sum_cost",
        "s.status",
        F.col("p.channel"),
        F.col("p.status").alias("order_status"),
        F.col("p.order_date"),
    )
)

result = (
    with_dims
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "running_count_cost",
        "running_sum_cost",
        "status",
        "channel",
        "order_status",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "EXTENDED").when(F.col("channel").rlike("^(store|partner)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("channel_zone"),
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("channel").asc_nulls_last())
)

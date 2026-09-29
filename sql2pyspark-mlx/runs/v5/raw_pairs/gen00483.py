from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("shipped_date", "id")
w_hist_2 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("prev_cost_rounded").desc(), "id")
w4 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w6 = Window.orderBy(F.col("event_seq").desc_nulls_first())

filtered = (
    df_shipments
    .filter(F.col("order_id").isin([2, 38]))
    .select(
        "*",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
        F.round("cost", 1).alias("cost_rounded"),
    )
)

with_windows = (
    filtered
    .withColumn("prev_cost_rounded", F.lag("cost_rounded").over(w_hist))
    .withColumn("next_cost_rounded", F.lead("cost_rounded").over(w_hist))
    .withColumn("next_cost", F.lead("cost").over(w_hist))
    .withColumn("prev_cost", F.lag("cost").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist_2))
)

with_windows_shipments = (
    with_windows
    .withColumn("row_seq_2", F.row_number().over(w3))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w4))
    .withColumn("row_seq_3", F.row_number().over(w_running))
)

matched_keys = (
    with_windows_shipments
    .select("carrier", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id").isin([6, 11, 15]))
        .select("carrier", "status")
    )
)

verified = (
    with_windows_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows_shipments_2 = verified.select("*", F.cume_dist().over(w6).alias("event_seq_cume_dist"))

valid = (
    with_windows_shipments_2
    .filter(F.col("order_id").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled",
        "cost_rounded",
        "prev_cost_rounded",
        "next_cost_rounded",
        "next_cost",
        "event_seq",
        "row_seq",
        "event_seq_cume_dist",
        F.round((F.col("event_seq") * 1.07), 2).alias("adjusted_event_seq"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled_2"),
    )
)

base = (
    valid
    .filter(F.col("delivered_date") >= F.lit("2023-08-17").cast("date"))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "cost_rounded",
        "prev_cost_rounded",
        "next_cost_rounded",
        "next_cost",
        "event_seq",
        "row_seq",
        "event_seq_cume_dist",
        F.round("event_seq_cume_dist", 0).alias("event_seq_cume_dist_rounded"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    base
    .withColumn("quarter_start", F.date_trunc("QUARTER", "delivered_date"))
    .groupingSets(
        [["quarter_start", "carrier"], ["quarter_start"], []],
        "quarter_start",
        "carrier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("status").alias("distinct_status"),
        F.max("cost").alias("max_cost"),
        F.avg("event_seq_cume_dist_rounded").alias("avg_event_seq_cume_dist_rounded"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
    )
    .filter(F.col("in_transit_rows") >= 1)
    .orderBy(F.col("quarter_start").asc_nulls_last())
)

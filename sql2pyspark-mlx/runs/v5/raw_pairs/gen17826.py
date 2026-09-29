from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id")
w_part = Window.orderBy("delivered_date", "id")
w_hist = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("event_seq").desc())
w_grp = Window.partitionBy("channel").orderBy("event_seq", "id")
w7 = Window.partitionBy("status").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel", "status").orderBy("event_seq", "id")
w9 = Window.partitionBy("channel").orderBy(F.col("row_num").desc(), "id")

valid = (
    df_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Kerry")))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
        F.concat("status", F.lit(", "), "carrier").alias("status_carrier"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.status").alias("order_status"), F.col("p.channel"))
)

with_windows = (
    enriched
    .select(
        "id",
        "status",
        "carrier_prefix",
        "channel",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w_part).alias("event_seq"),
        F.first("order_status", ignorenulls=True).over(w_hist).alias("first_known_order_status"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.lag("row_seq").over(w_grp).alias("prev_row_seq"),
        F.lead("row_seq").over(w_grp).alias("next_row_seq"),
        F.lead("seq_no").over(w_grp).alias("next_seq_no"),
        F.row_number().over(w_grp).alias("event_seq_2"),
        F.first("event_seq", ignorenulls=True).over(w7).alias("first_known_event_seq"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

latest_shipments = (
    latest
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

result = (
    latest_shipments
    .select(
        "id",
        "status",
        "carrier_prefix",
        "channel",
        "row_seq",
        "event_seq",
        "row_num",
        "prev_row_seq",
        "next_row_seq",
    )
    .orderBy(F.col("event_seq").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.orderBy("prev_days_shipped_date_to_delivered_date_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("delivered_date", "id")
w4 = Window.orderBy(F.col("prev_days_shipped_date_to_delivered_date").desc_nulls_first())
w5 = Window.orderBy(F.col("prev_days_shipped_date_to_delivered_date_2").desc_nulls_last())
w6 = Window.orderBy(F.col("prev_days_shipped_date_to_delivered_date_2").desc())
w7 = Window.orderBy(F.col("event_seq").asc_nulls_last())

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-05-26").cast("date")) & (F.col("cost") <= 78))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "delivered_date",
        "carrier_prefix",
        F.lag("days_shipped_date_to_delivered_date", 1).over(w_hist).alias("prev_days_shipped_date_to_delivered_date"),
        F.lag("days_shipped_date_to_delivered_date", 1).over(w_hist).alias("prev_days_shipped_date_to_delivered_date_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("prev_days_shipped_date_to_delivered_date_2").between(28, 64))
    .select(
        "*",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
    )
)

enriched_shipments = (
    cleaned
    .filter(F.col("days_since_delivered_date") > 78)
    .select(
        "*",
        F.sum("prev_days_shipped_date_to_delivered_date").over(w2).alias("running_prev_days_shipped_date_to_delivered_date"),
        F.row_number().over(w3).alias("event_seq"),
        F.percent_rank().over(w4).alias("prev_days_shipped_date_to_delivered_date_percent_rank"),
        F.dense_rank().over(w5).alias("prev_days_shipped_date_to_delivered_date_2_dense_rank"),
    )
)

remaining_keys = (
    enriched_shipments
    .select("id")
    .exceptAll(
        df_shipments
        .filter(~F.col("carrier").isin(["Kerry", "FedEx", "Flash"]))
        .select("id")
    )
)

reconciled = (
    enriched_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "prev_days_shipped_date_to_delivered_date",
        "prev_days_shipped_date_to_delivered_date_2",
        "days_since_delivered_date",
        "delivered_date_plus_months",
        "prev_days_shipped_date_to_delivered_date_2_dense_rank",
        F.dense_rank().over(w6).alias("prev_days_shipped_date_to_delivered_date_2_dense_rank_2"),
        F.cume_dist().over(w7).alias("event_seq_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "prev_days_shipped_date_to_delivered_date",
        "prev_days_shipped_date_to_delivered_date_2",
        "days_since_delivered_date",
        "delivered_date_plus_months",
        "prev_days_shipped_date_to_delivered_date_2_dense_rank_2",
        "event_seq_cume_dist",
    )
    .orderBy(F.col("prev_days_shipped_date_to_delivered_date_2").desc_nulls_first())
)

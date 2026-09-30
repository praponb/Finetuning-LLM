from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date_plus_11d", "id")
w3 = Window.orderBy("next_cost_2")
w_ordered = Window.partitionBy("carrier").orderBy("event_seq", "id")
w5 = Window.partitionBy("carrier", "status").orderBy(F.col("next_cost_2").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy("next_next_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status")
w8 = Window.partitionBy("carrier", "status").orderBy("next_next_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("next_cost_2").desc(), "id")
w10 = Window.partitionBy("carrier").orderBy(F.col("next_cost_2").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.date_add("shipped_date", 11).alias("shipped_date_plus_11d"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lead("cost").over(w).alias("next_cost"),
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("event_seq"),
        F.lead("cost", 2).over(w2).alias("next_cost_2"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        "resolved_carrier",
        "next_cost",
        "prev_cost",
        "event_seq",
        "next_cost_2",
        F.percent_rank().over(w3).alias("next_cost_2_percent_rank"),
        F.lead("next_cost", 2).over(w_ordered).alias("next_next_cost"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

metrics_shipments = (
    enriched
    .filter(F.col("prev_cost") < 40)
    .select(
        "*",
        F.first("carrier", ignorenulls=True).over(w6).alias("first_known_carrier"),
        F.avg("row_seq").over(w_grp).alias("status_avg_row_seq"),
        F.last("prev_cost", ignorenulls=True).over(w8).alias("last_known_prev_cost"),
    )
)

scored = (
    metrics_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        "next_cost",
        "event_seq",
        "next_cost_2",
        "row_seq",
        "first_known_carrier",
        "status_avg_row_seq",
        "last_known_prev_cost",
        F.sum("status_avg_row_seq").over(w6).alias("running_status_avg_row_seq"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.ntile(4).over(w10).alias("next_cost_2_bucket"),
    )
)

result = (
    scored
    .groupBy("order_id")
    .pivot("status", ["LOST", "DELIVERED"])
    .agg(F.max("row_seq_2"))
)

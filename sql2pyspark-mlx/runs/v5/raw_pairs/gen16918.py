from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status")
w_recent = Window.partitionBy("carrier").orderBy("cost", "id")
w4 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("prev_cost_avg_6").asc_nulls_last())
w6 = Window.orderBy(F.col("prev_cost_avg_6").desc_nulls_first())
w7 = Window.orderBy(F.col("prev_cost_avg_6_dense_rank").asc_nulls_last())

cleaned = (
    df_shipments
    .filter(F.month("shipped_date").isin([2, 6, 9]))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.trim("carrier").alias("clean_carrier"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("status") == "IN_TRANSIT")
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_6"),
        F.max("cost").over(w2).alias("status_max_cost"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "carrier",
        "delivered_date",
        "clean_carrier",
        "cost_avg_6",
        F.lag("cost_avg_6").over(w_recent).alias("prev_cost_avg_6"),
        F.lead("cost_avg_6").over(w_recent).alias("next_cost_avg_6"),
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.lead("cost").over(w_recent).alias("next_cost"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

cleaned_shipments = (
    scored
    .filter(F.col("delivered_date") >= F.lit("2023-09-06").cast("date"))
    .select(
        "id",
        "delivered_date",
        "clean_carrier",
        "prev_cost_avg_6",
        "prev_cost",
        "event_seq",
        F.substring("carrier", 1, 2).alias("carrier_prefix"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

scored_shipments = (
    cleaned_shipments
    .withColumn("first_known_prev_cost", F.first("prev_cost", ignorenulls=True).over(w4))
    .withColumn("prev_cost_avg_6_dense_rank", F.dense_rank().over(w5))
)

remaining_keys = (
    scored_shipments
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2023-03-01", "2025-09-01"))
        .select("id")
    )
)

reconciled = (
    scored_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_shipments = (
    reconciled
    .select(
        "prev_cost",
        "event_seq",
        "carrier_prefix",
        F.dense_rank().over(w6).alias("prev_cost_avg_6_dense_rank_2"),
        F.dense_rank().over(w7).alias("prev_cost_avg_6_dense_rank_dense_rank"),
    )
)

result = (
    with_windows_shipments
    .select("prev_cost", "event_seq", "carrier_prefix", "prev_cost_avg_6_dense_rank_2")
    .orderBy(F.col("event_seq").desc_nulls_first())
)

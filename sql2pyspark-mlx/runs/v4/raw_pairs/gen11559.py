from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("delivered_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("delivered_date_dayofweek_times_cost").desc(), "id")
w_running = Window.partitionBy("carrier", "status").orderBy("delivered_date_dayofweek", "id")
w6 = Window.partitionBy("status").orderBy("delivered_date_dayofweek", "id")
w7 = Window.partitionBy("status")
w8 = Window.orderBy(F.col("row_seq").asc_nulls_last())
w9 = Window.partitionBy("status", "carrier").orderBy(F.col("row_seq").desc_nulls_last())

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-09").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("delivered_date_dayofweek", F.dayofweek("delivered_date"))
    .withColumn("carrier_prefix", F.substring("carrier", 1, 3))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("OTHER")))
)

scored = (
    filtered
    .withColumn("first_known_delivered_date_dayofweek", F.first("delivered_date_dayofweek", ignorenulls=True).over(w_ordered))
    .withColumn("order_id_min_delivered_date_dayofweek", F.min("delivered_date_dayofweek").over(w2))
)

filtered_shipments = (
    scored
    .filter(F.col("shipped_date") > F.lit("2023-08-01").cast("date"))
    .withColumn("shipped_date_label", F.date_format("shipped_date", "yyyy-MM"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

cleaned = (
    filtered_shipments
    .filter(F.col("cost") < 48)
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "delivered_date_dayofweek",
        "carrier_prefix",
        "resolved_carrier",
        "first_known_delivered_date_dayofweek",
        "shipped_date_label",
        "carrier_filled",
        (F.col("delivered_date_dayofweek") * F.col("cost")).alias("delivered_date_dayofweek_times_cost"),
    )
)

scored_shipments = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("delivered_date_dayofweek_times_cost_bucket", F.ntile(5).over(w4))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("prev_first_known_delivered_date_dayofweek", F.lag("first_known_delivered_date_dayofweek", 1).over(w6))
)

remaining_keys = (
    scored_shipments
    .select("carrier")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") <= "2023-10-01")
        .select("carrier")
    )
)

verified = (
    scored_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4).alias("row_seq_share"),
        F.rank().over(w8).alias("row_seq_rank"),
        F.rank().over(w9).alias("row_seq_rank_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "resolved_carrier",
        "shipped_date_label",
        "carrier_filled",
        "row_seq",
        "delivered_date_dayofweek_times_cost_bucket",
    )
    .orderBy("cost", "carrier")
)

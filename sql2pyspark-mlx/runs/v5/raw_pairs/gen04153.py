from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id")
w3 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy("shipped_date_period")
w5 = Window.orderBy("seq_no_2", "id")
w6 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.lead("cost").over(w_part).alias("next_cost"),
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

filtered = (
    deduped
    .filter(F.col("carrier").isNotNull() & (F.col("next_cost") <= 26))
    .select(
        "*",
        F.concat_ws(",", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

filtered_shipments = (
    filtered
    .filter(F.col("shipped_date") >= F.lit("2023-03-31").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "next_cost",
        "prev_cost",
        "seq_no",
        "seq_no_2",
        "row_num",
        "carrier_status_key",
        "resolved_carrier",
        F.date_trunc("WEEK", "shipped_date").alias("shipped_date_period"),
    )
)

latest = (
    filtered_shipments
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

enriched_shipments = (
    latest
    .filter(F.col("seq_no").isNotNull())
    .select(
        "id",
        "next_cost",
        "prev_cost",
        "seq_no",
        "carrier_status_key",
        "rank_in_group",
        F.ntile(3).over(w5).alias("seq_no_2_bucket"),
        F.sum("seq_no").over(w6).alias("cumulative_seq_no"),
    )
)

result = (
    enriched_shipments
    .groupBy(
        F.when(F.col("next_cost") >= 103, "LARGE").otherwise("SMALL").alias("next_cost_tier"),
        "seq_no",
    )
    .agg(
        F.percentile_approx("seq_no_2_bucket", 0.95).alias("seq_no_2_bucket_p95"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("seq_no_2_bucket", [0.25, 0.5, 0.75]).alias("seq_no_2_bucket_quartiles"),
    )
    .filter(F.col("seq_no_2_bucket_p95") > 0)
    .orderBy(F.col("event_count").desc())
)

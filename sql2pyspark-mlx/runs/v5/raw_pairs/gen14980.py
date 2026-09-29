from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date_period", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("shipped_date_period", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist = Window.orderBy("island_id", "id")
w6 = Window.partitionBy("status")
w_running = Window.partitionBy("status").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("island_id_bucket", "id")
w9 = Window.partitionBy("status").orderBy("status_sum_island_id_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("island_id_count_7d", "id")
w11 = Window.partitionBy("cumulative_prev_island_id_bucket_tier").orderBy(F.col("event_seq_p90").desc())

base = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-07-10").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        F.lower("status").alias("status_lower"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

base_shipments = (
    base
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-07-03"))) & (F.col("delivered_date") >= "2024-03-26"))
    .withColumn("shipped_date_period", F.date_trunc("year", "shipped_date"))
    .withColumn("shipped_date_month_end", F.last_day("shipped_date"))
)

flagged = (
    base_shipments
    .withColumn("is_new_session", F.when(F.col("shipped_date_period") - F.lag("shipped_date_period").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "status",
        "status_lower",
        "shipped_date_period",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

scored = (
    islands
    .filter(F.hour("shipped_date_period").between(6, 9))
    .select(
        "id",
        "status",
        "status_lower",
        "shipped_date_period",
        "island_id",
        F.avg("island_id").over(w3).alias("island_id_avg_6_prior"),
        F.count("island_id").over(w4).alias("island_id_count_7d"),
        F.ntile(5).over(w_hist).alias("island_id_bucket"),
        F.avg("island_id").over(w6).alias("status_avg_island_id"),
        F.sum("island_id").over(w_running).alias("cumulative_island_id"),
    )
)

scored_shipments = (
    scored
    .select(
        "id",
        "status",
        "status_lower",
        "island_id_count_7d",
        F.sum("island_id_bucket").over(w6).alias("status_sum_island_id_bucket"),
        F.lag("island_id_bucket", 1).over(w8).alias("prev_island_id_bucket"),
    )
)

metrics = (
    scored_shipments
    .select(
        "id",
        "status",
        "status_sum_island_id_bucket",
        F.sum("prev_island_id_bucket").over(w9).alias("cumulative_prev_island_id_bucket"),
        F.row_number().over(w10).alias("event_seq"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("cumulative_prev_island_id_bucket") >= 39, "HIGH").otherwise("LOW").alias("cumulative_prev_island_id_bucket_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("status_sum_island_id_bucket", [0.25, 0.5, 0.75]).alias("status_sum_island_id_bucket_quartiles"),
        F.percentile_approx("status_sum_island_id_bucket", [0.25, 0.5, 0.75]).alias("status_sum_island_id_bucket_quartiles_2"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .withColumn("event_seq_p90_dense_rank", F.dense_rank().over(w11))
)

result = (
    grouped
    .select(
        "cumulative_prev_island_id_bucket_tier",
        "status",
        "status_sum_island_id_bucket_quartiles",
        "status_sum_island_id_bucket_quartiles_2",
        "event_seq_p90",
        "delivered_rows",
        "num_status",
        "event_seq_p90_dense_rank",
        F.when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)

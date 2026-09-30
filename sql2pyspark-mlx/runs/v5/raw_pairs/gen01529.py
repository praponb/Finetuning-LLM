from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_length", "id")
w2 = Window.orderBy("delivered_date", "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "order_id")
w_running = Window.partitionBy("order_id").orderBy("status_count_status_length_share", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy("status_count_status_length_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("running_count_row_seq", "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("cost") >= 69))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.length("status").alias("status_length"),
        F.trim("carrier").alias("clean_carrier"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.ntile(5).over(w).alias("status_length_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.count("status_length").over(w3).alias("status_count_status_length"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status_length",
        "clean_carrier",
        "status_length_bucket",
        "row_seq",
        "status_count_status_length",
        F.min("row_seq").over(w4).alias("running_min_row_seq"),
        F.max("row_seq").over(w4).alias("running_max_row_seq"),
        F.sum("row_seq").over(w4).alias("running_sum_row_seq"),
        F.count("row_seq").over(w4).alias("running_count_row_seq"),
        F.round((F.col("status_count_status_length") / F.sum("status_count_status_length").over(w5)), 4).alias("status_count_status_length_share"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("row_seq").over(w_running).alias("row_seq_avg_2"),
        F.last("status_length", ignorenulls=True).over(w_hist).alias("last_known_status_length"),
        F.lead("running_count_row_seq", 2).over(w8).alias("next_running_count_row_seq"),
    )
)

stats = (
    with_windows
    .withColumn("quarter_start", F.date_trunc("QUARTER", "delivered_date"))
    .withColumn("running_count_row_seq_tier", F.when(F.col("running_count_row_seq") >= 16, "HIGH").otherwise("LOW"))
    .cube("quarter_start", "running_count_row_seq_tier")
    .agg(
        F.round(F.avg("status_count_status_length_share"), 2).alias("avg_status_count_status_length_share"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_max_row_seq", [0.25, 0.5, 0.75]).alias("running_max_row_seq_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "quarter_start",
        "running_count_row_seq_tier",
        "avg_status_count_status_length_share",
        "running_max_row_seq_quartiles",
        "grouping_level",
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)

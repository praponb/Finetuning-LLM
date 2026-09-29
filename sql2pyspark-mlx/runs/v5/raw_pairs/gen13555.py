from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w_recent_2 = Window.partitionBy("status", "order_id")
w3 = Window.partitionBy("carrier").orderBy("row_seq", "id")
w4 = Window.partitionBy("carrier").orderBy("delivered_date_period", "id")
w5 = Window.partitionBy("carrier").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("session_no").desc())
w8 = Window.orderBy("running_sum_session_no", "id")
w9 = Window.partitionBy("running_count_session_no").orderBy(F.col("num_rows").desc())

enriched = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.row_number().over(w_recent).alias("row_seq"),
        F.sum("cost").over(w_recent_2).alias("status_sum_cost"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    deduped
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") >= F.to_date(F.lit("2023-08-17"))))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "row_seq",
        "status_sum_cost",
        "row_num",
        F.date_trunc("WEEK", "delivered_date").alias("delivered_date_period"),
    )
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "carrier",
        "delivered_date",
        "row_num",
        "delivered_date_period",
        F.sum("new_group_flag").over(w5).alias("session_no"),
    )
)

cleaned_shipments = (
    sessionized
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "delivered_date",
        "row_num",
        "delivered_date_period",
        "session_no",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
)

enriched_shipments = (
    cleaned_shipments
    .select(
        "id",
        "delivered_date",
        F.sum("session_no").over(w_grp).alias("running_sum_session_no"),
        F.avg("session_no").over(w_grp).alias("running_avg_session_no"),
        F.count("session_no").over(w_grp).alias("running_count_session_no"),
        F.max("session_no").over(w_grp).alias("running_max_session_no"),
        F.rank().over(w_hist).alias("session_no_rank"),
    )
)

with_windows = (
    enriched_shipments
    .filter(F.col("running_sum_session_no").between(14, 17))
    .withColumn("prev_running_sum_session_no", F.lag("running_sum_session_no").over(w8))
    .withColumn("next_running_sum_session_no", F.lead("running_sum_session_no").over(w8))
    .withColumn("next_running_avg_session_no", F.lead("running_avg_session_no").over(w8))
    .withColumn("row_seq", F.row_number().over(w8))
)

result = (
    with_windows
    .groupBy(
        "running_count_session_no",
        F.date_trunc("quarter", "delivered_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("running_count_session_no", 0.5).alias("running_count_session_no_p50"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_running_avg_session_no", [0.25, 0.5, 0.75]).alias("next_running_avg_session_no_quartiles"),
    )
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w9))
    .orderBy(F.col("row_count").desc_nulls_first())
)

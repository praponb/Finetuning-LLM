from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code_high", "id", "key").rowsBetween(-2, -1)
w2 = Window.partitionBy("key").orderBy("is_flagged", "id", "key")
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("key").orderBy("is_flagged_avg_6", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("key").orderBy(F.col("status_code_high_count_2h").asc_nulls_last())
w7 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w8 = Window.partitionBy("key").orderBy("row_seq", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("key").orderBy("greatest_next_is_flagged_avg_2_prior_prev_is_flagged_avg_2_prior", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    lines
    .filter(F.col("key").isin(["trace", "region"]))
    .select(
        "id",
        "log_ts",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    unpacked
    .filter(F.col("status_code_high") >= 1)
    .select(
        "id",
        "log_ts",
        "key",
        F.avg("is_flagged").over(w).alias("is_flagged_avg_2_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.count("status_code_high").over(w3).alias("status_code_high_count_2h"),
        F.avg("is_flagged").over(w4).alias("is_flagged_avg_6"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "log_ts",
        "key",
        "is_flagged_avg_2_prior",
        "seq_no",
        F.sum("is_flagged_avg_2_prior").over(w_running).alias("cumulative_is_flagged_avg_2_prior"),
        F.dense_rank().over(w6).alias("status_code_high_count_2h_dense_rank"),
    )
)

with_windows_app_logs = (
    enriched
    .select(
        "id",
        "log_ts",
        "key",
        "cumulative_is_flagged_avg_2_prior",
        F.lag("is_flagged_avg_2_prior").over(w7).alias("prev_is_flagged_avg_2_prior"),
        F.lead("is_flagged_avg_2_prior").over(w7).alias("next_is_flagged_avg_2_prior"),
        F.lead("cumulative_is_flagged_avg_2_prior").over(w7).alias("next_cumulative_is_flagged_avg_2_prior"),
        F.row_number().over(w7).alias("row_seq"),
        F.lead("seq_no", 2).over(w7).alias("next_seq_no"),
    )
)

cleaned = (
    with_windows_app_logs
    .filter(F.col("next_seq_no").between(3, 8) & (F.col("log_ts") >= F.lit("2025-06-01").cast("date")))
    .select(
        "id",
        "log_ts",
        "key",
        "cumulative_is_flagged_avg_2_prior",
        "prev_is_flagged_avg_2_prior",
        "next_is_flagged_avg_2_prior",
        "next_cumulative_is_flagged_avg_2_prior",
        "row_seq",
        "next_seq_no",
        F.greatest("next_is_flagged_avg_2_prior", "prev_is_flagged_avg_2_prior").alias("greatest_next_is_flagged_avg_2_prior_prev_is_flagged_avg_2_prior"),
    )
)

with_windows_app_logs_2 = (
    cleaned
    .select(
        "*",
        F.max("row_seq").over(w8).alias("running_max_row_seq"),
        F.count("row_seq").over(w8).alias("running_count_row_seq"),
        F.sum("row_seq").over(w8).alias("running_sum_row_seq"),
        F.lag("row_seq", 1).over(w9).alias("prev_row_seq"),
    )
)

result = (
    with_windows_app_logs_2
    .select(
        "id",
        "log_ts",
        "key",
        "cumulative_is_flagged_avg_2_prior",
        "prev_is_flagged_avg_2_prior",
        "next_is_flagged_avg_2_prior",
        "next_cumulative_is_flagged_avg_2_prior",
        "next_seq_no",
        "greatest_next_is_flagged_avg_2_prior_prev_is_flagged_avg_2_prior",
    )
    .orderBy(F.col("log_ts").asc_nulls_first())
)

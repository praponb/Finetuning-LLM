from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("status_code", "id", "k")
w4 = Window.partitionBy("level").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "k")
w_part = Window.partitionBy("host")
w_grp = Window.partitionBy("host", "k").orderBy("row_seq", "id", "k")
w8 = Window.orderBy(F.col("status_code_bucket").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.sum("status_code").over(w).alias("cumulative_status_code"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("status_code").over(w4).alias("cumulative_status_code_2"),
    )
)

with_windows_app_logs = (
    with_windows
    .filter(F.col("row_seq") > 5)
    .select(
        "*",
        F.ntile(3).over(w5).alias("status_code_bucket"),
        F.avg("cumulative_status_code_2").over(w_part).alias("host_avg_cumulative_status_code_2"),
    )
)

enriched = (
    with_windows_app_logs
    .select(
        "*",
        F.lag("host_avg_cumulative_status_code_2").over(w_grp).alias("prev_host_avg_cumulative_status_code_2"),
        F.lag("status_code_bucket").over(w_grp).alias("prev_status_code_bucket"),
        F.row_number().over(w_grp).alias("row_seq_2"),
        F.dense_rank().over(w8).alias("status_code_bucket_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("status_code") >= 265)
    .select(
        "id",
        "service",
        "level",
        "k",
        "attr_value",
        "first_known_k",
        "row_seq",
        "cumulative_status_code_2",
        "prev_host_avg_cumulative_status_code_2",
        "prev_status_code_bucket",
        "status_code_bucket_dense_rank",
        (F.col("row_seq_2") * F.col("status_code_bucket_dense_rank")).alias("row_seq_2_times_status_code_bucket_dense_rank"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("prev_host_avg_cumulative_status_code_2") >= 2137, "HIGH").otherwise("LOW").alias("prev_host_avg_cumulative_status_code_2_tier"),
        "service",
    )
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("k") == "version").alias("version_events"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
    )
    .orderBy(F.col("version_count").desc())
)

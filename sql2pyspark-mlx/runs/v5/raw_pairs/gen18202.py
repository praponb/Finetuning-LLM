from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code_plus_latency_ms", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w4 = Window.orderBy("service_length", "id", "attr_key").rowsBetween(-3, -1)
w_hist = Window.partitionBy("host", "attr_key").orderBy("prev_cumulative_status_code_plus_latency_ms", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy(F.col("row_seq").desc(), "id", "attr_key")
w7 = Window.partitionBy("host").orderBy("row_seq", "id", "attr_key")
w8 = Window.partitionBy("host").orderBy(F.col("seq_no").desc_nulls_last())
w9 = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

filtered = (
    exploded
    .filter(F.col("service") != "auth")
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        F.length("service").alias("service_length"),
        F.concat_ws("-", "service", "host").alias("service_host_key"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

base = (
    filtered
    .filter(F.col("attr_key").isNotNull() & F.col("attr_key").isin(["version", "region", "trace"]))
    .select(
        "*",
        (F.col("status_code") + F.col("latency_ms")).alias("status_code_plus_latency_ms"),
        F.round("latency_ms", 0).alias("latency_ms_rounded"),
    )
)

with_windows = (
    base
    .withColumn("next_status_code_plus_latency_ms", F.lead("status_code_plus_latency_ms").over(w))
    .withColumn("prev_status_code", F.lag("status_code").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("cumulative_status_code_plus_latency_ms", F.sum("status_code_plus_latency_ms").over(w2))
)

with_windows_app_logs = (
    with_windows
    .select(
        "id",
        "host",
        "latency_ms",
        "attr_key",
        "service_host_key",
        "status_code_plus_latency_ms",
        "latency_ms_rounded",
        "next_status_code_plus_latency_ms",
        "row_seq",
        F.lag("latency_ms").over(w_part).alias("prev_latency_ms"),
        F.lag("cumulative_status_code_plus_latency_ms").over(w_part).alias("prev_cumulative_status_code_plus_latency_ms"),
        F.row_number().over(w_part).alias("event_seq"),
        F.avg("status_code_plus_latency_ms").over(w4).alias("status_code_plus_latency_ms_avg_3_prior"),
    )
)

scored = (
    with_windows_app_logs
    .select(
        "id",
        "host",
        "attr_key",
        "service_host_key",
        "status_code_plus_latency_ms",
        "latency_ms_rounded",
        "row_seq",
        "prev_latency_ms",
        "prev_cumulative_status_code_plus_latency_ms",
        F.lag("prev_latency_ms").over(w_hist).alias("prev_prev_latency_ms"),
        F.lead("prev_latency_ms").over(w_hist).alias("next_prev_latency_ms"),
        F.lead("row_seq").over(w_hist).alias("next_row_seq"),
        F.row_number().over(w_hist).alias("row_seq_2"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

scored_app_logs = (
    scored
    .select(
        "*",
        F.row_number().over(w7).alias("seq_no_2"),
        F.lag("next_prev_latency_ms", 2).over(w_hist).alias("prev_next_prev_latency_ms"),
        F.percent_rank().over(w8).alias("seq_no_percent_rank"),
        F.round((F.col("prev_prev_latency_ms") / F.sum("prev_prev_latency_ms").over(w9)), 4).alias("prev_prev_latency_ms_share"),
    )
)

result = (
    scored_app_logs
    .groupBy("seq_no", "attr_key", "host")
    .agg(
        F.sum("row_seq_2").alias("total_row_seq_2"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("seq_no_percent_rank")).otherwise(0)).alias("region_seq_no_percent_rank"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.sum(F.when(F.col("host") == "h1", F.col("prev_cumulative_status_code_plus_latency_ms")).otherwise(0)).alias("h1_prev_cumulative_status_code_plus_latency_ms"),
    )
    .filter(F.col("cnt") > 1)
)

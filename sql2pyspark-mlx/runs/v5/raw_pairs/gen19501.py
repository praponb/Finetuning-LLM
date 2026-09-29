from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last(), "id", "key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("service").orderBy("latency_ms", "id", "key").rowsBetween(-5, -1)
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_last())
w5 = Window.partitionBy("service", "key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("key").orderBy("service_min_latency_ms", "id", "key")
w7 = Window.partitionBy("key").orderBy("prev_latency_ms", "id", "key").rowsBetween(-2, -1)
w8 = Window.orderBy(F.col("latency_ms").desc())
w9 = Window.partitionBy("key").orderBy("next_service_min_latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

deduped = (
    exploded
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .select(
        "*",
        F.min("latency_ms").over(w2).alias("service_min_latency_ms"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_5_prior"),
    )
)

scored_app_logs = (
    scored
    .filter(F.col("key").isin(["version", "region"]))
    .select(
        "id",
        "latency_ms",
        "key",
        "attr_value",
        "service_min_latency_ms",
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
        F.sum("latency_ms_avg_5_prior").over(w5).alias("running_latency_ms_avg_5_prior"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
    )
)

metrics = (
    scored_app_logs
    .select(
        "id",
        "latency_ms",
        "key",
        "latency_ms_share",
        F.lag("latency_ms").over(w_part).alias("prev_latency_ms"),
        F.lag("service_min_latency_ms").over(w_part).alias("prev_service_min_latency_ms"),
        F.lead("service_min_latency_ms").over(w_part).alias("next_service_min_latency_ms"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

scored_app_logs_2 = (
    metrics
    .select(
        "id",
        "latency_ms",
        "key",
        "latency_ms_share",
        "seq_no",
        F.avg("next_service_min_latency_ms").over(w7).alias("next_service_min_latency_ms_avg_2_prior"),
        F.percent_rank().over(w8).alias("latency_ms_percent_rank"),
        F.sum("latency_ms").over(w9).alias("cumulative_latency_ms"),
        F.round((F.col("next_service_min_latency_ms") / F.sum("next_service_min_latency_ms").over(w10)), 4).alias("next_service_min_latency_ms_share"),
    )
)

valid = (
    scored_app_logs_2
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "latency_ms",
        "key",
        "seq_no",
        "next_service_min_latency_ms_avg_2_prior",
        "latency_ms_percent_rank",
        "cumulative_latency_ms",
        "next_service_min_latency_ms_share",
        F.round((F.col("cumulative_latency_ms") * 1.15), 2).alias("cumulative_latency_ms_adjusted"),
        (F.col("latency_ms") + F.col("seq_no")).alias("latency_ms_plus_seq_no"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

result = (
    valid
    .groupBy(
        "seq_no",
        F.when(F.col("cumulative_latency_ms") >= 9018, "PREMIUM").otherwise("STANDARD").alias("cumulative_latency_ms_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.percentile_approx("next_service_min_latency_ms_share", [0.25, 0.5, 0.75]).alias("next_service_min_latency_ms_share_quartiles"),
    )
    .filter(F.col("trace_rows") >= 1)
)

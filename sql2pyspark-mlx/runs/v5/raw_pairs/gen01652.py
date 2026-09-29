from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_part_2 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy(F.col("status_code_avg_2h").desc(), "id", "attr_key")
w_grp = Window.partitionBy("host").orderBy("status_code_avg_2h", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("host").orderBy("next_latency_ms", "id", "attr_key").rowsBetween(-6, -1)
w7 = Window.partitionBy("attr_key").orderBy("running_count_next_latency_ms", "id", "attr_key").rowsBetween(-3, -1)
w8 = Window.partitionBy("attr_key").orderBy("next_latency_ms_avg_6_prior_rounded", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("attr_key").orderBy("status_code_avg_2h", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("status_code_bucket", "id", "attr_key").rowsBetween(-2, -1)
w11 = Window.partitionBy("attr_key").orderBy(F.col("status_code_avg_2h_avg_3_prior").desc(), "id", "attr_key")
w12 = Window.partitionBy("status_code_avg_2h_avg_2_prior_tier").orderBy(F.col("status_code_avg_2h_per_region").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "host",
        "attr_key",
        F.lead("latency_ms", 1).over(w_part).alias("next_latency_ms"),
        F.avg("status_code").over(w2).alias("status_code_avg_2h"),
        F.ntile(4).over(w_part_2).alias("status_code_bucket"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .select(
        "id",
        "attr_key",
        "next_latency_ms",
        "status_code_avg_2h",
        "status_code_bucket",
        F.max("next_latency_ms").over(w_grp).alias("running_max_next_latency_ms"),
        F.min("next_latency_ms").over(w_grp).alias("running_min_next_latency_ms"),
        F.count("next_latency_ms").over(w_grp).alias("running_count_next_latency_ms"),
        F.avg("next_latency_ms").over(w6).alias("next_latency_ms_avg_6_prior"),
    )
)

cleaned = (
    scored
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "attr_key",
        "next_latency_ms",
        "status_code_avg_2h",
        "status_code_bucket",
        "running_max_next_latency_ms",
        "running_count_next_latency_ms",
        F.round("next_latency_ms_avg_6_prior", 0).alias("next_latency_ms_avg_6_prior_rounded"),
    )
)

enriched_app_logs = (
    cleaned
    .select(
        "id",
        "attr_key",
        "next_latency_ms",
        "status_code_avg_2h",
        "running_max_next_latency_ms",
        "running_count_next_latency_ms",
        "next_latency_ms_avg_6_prior_rounded",
        F.avg("status_code_avg_2h").over(w7).alias("status_code_avg_2h_avg_3_prior"),
        F.first("next_latency_ms", ignorenulls=True).over(w8).alias("first_known_next_latency_ms"),
        F.first("running_max_next_latency_ms", ignorenulls=True).over(w9).alias("first_known_running_max_next_latency_ms"),
        F.avg("status_code_avg_2h").over(w10).alias("status_code_avg_2h_avg_2_prior"),
    )
)

latest = (
    enriched_app_logs
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.when(F.col("status_code_avg_2h_avg_2_prior") >= 440, "LARGE").otherwise("SMALL").alias("status_code_avg_2h_avg_2_prior_tier"),
        F.when(F.col("first_known_next_latency_ms") >= 2515, "HIGH").otherwise("LOW").alias("first_known_next_latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_rows"),
        F.round(F.sum("status_code_avg_2h") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_avg_2h_per_region"),
        F.sum(F.when(F.col("attr_key") == "version", F.col("running_count_next_latency_ms")).otherwise(0)).alias("version_running_count_next_latency_ms"),
    )
    .withColumn("status_code_avg_2h_per_region_dense_rank", F.dense_rank().over(w12))
)

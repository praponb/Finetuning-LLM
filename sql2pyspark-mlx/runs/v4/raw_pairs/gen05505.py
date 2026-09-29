from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("latency_ms", "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_first())
w_grp = Window.partitionBy("level").orderBy(F.col("latency_ms").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

deduped = (
    flattened
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    deduped
    .select("host", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code").between(344, 404))
        .select("host", "level")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
        F.dense_rank().over(w_grp).alias("latency_ms_dense_rank_2"),
    )
)

valid = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01 11:00:00").cast("timestamp")) & F.col("k").isNotNull() & (F.col("dedup_rank") < 1))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "value",
        "dedup_rank",
        "latency_ms_dense_rank",
        "latency_ms_dense_rank_2",
        F.initcap(F.trim("host")).alias("host_title"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

result = (
    valid
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .groupingSets(
        [["k", "level", "activity_day"], ["k", "level"], ["k"]],
        "k",
        "level",
        "activity_day",
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes_2"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sort_array(F.collect_set("level")).alias("level_list_2"),
        F.max("log_ts").alias("last_log_ts"),
        F.grouping("k").alias("gid"),
    )
)

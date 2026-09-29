from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w3 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("hour_window").orderBy(F.col("distinct_attr_keys").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

ranked = (
    unpacked
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

valid = (
    islands
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .withColumn("service_prefix", F.substring("service", 1, 2))
    .withColumn("service_title", F.initcap(F.trim("service")))
)

matched_keys = (
    valid
    .select("id", "service")
    .subtract(
        df_app_logs
        .filter(F.col("host").isin(["h2", "h1", "h3"]))
        .select("id", "service")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("service", "id")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["ERROR", "DEBUG"]))
        .select("service", "id")
    )
)

verified_app_logs = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified_app_logs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "service")
    .agg(
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .withColumn("distinct_attr_keys_dense_rank", F.dense_rank().over(w4))
    .orderBy(F.col("hour_window").desc_nulls_first())
)

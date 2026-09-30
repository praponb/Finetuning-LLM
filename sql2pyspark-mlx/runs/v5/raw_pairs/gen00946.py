from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "k").orderBy(F.col("latency_ms").desc(), "id", "k")
w3 = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "k")
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .filter(F.col("latency_ms").between(899, 1324))
    .select(
        "*",
        F.last("k", ignorenulls=True).over(w).alias("last_known_k"),
        F.ntile(5).over(w2).alias("latency_ms_bucket"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter(F.col("k").isNotNull() & (F.col("status_code") > 268))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "v",
        "last_known_k",
        "latency_ms_bucket",
        "row_num",
        F.substring("k", 1, 4).alias("k_prefix"),
    )
)

valid = (
    base
    .filter(F.col("k").isNotNull() & (F.col("status_code") >= 358))
    .select(
        "*",
        F.coalesce("k", F.lit("unassigned")).alias("k_filled"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled_2"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w4) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        "v",
        "row_num",
        "k_prefix",
        F.sum("is_new_burst").over(w5).alias("session_id"),
    )
)

result = (
    islands
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "HIGH").otherwise("LOW"))
    .rollup("session_id_tier", "session_id")
    .agg(
        F.sum(F.when(F.col("k") == "version", F.col("status_code"))).alias("version_total_status_code"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.countDistinct("k").alias("distinct_ks"),
        F.grouping("session_id_tier").alias("gid"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("island_id").desc(), "id", "key")
w4 = Window.partitionBy("level", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

with_gaps = (
    exploded
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

with_windows = (
    islands
    .select(
        "*",
        F.ntile(4).over(w3).alias("island_id_bucket"),
        F.avg("island_id").over(w4).alias("island_id_avg_2h"),
    )
)

remaining_keys = (
    with_windows
    .select("host", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("status_code").between(261, 328))
        .select("host", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("island_id_bucket") >= 7, "LARGE").otherwise("SMALL").alias("island_id_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_count"),
        F.max("island_id_avg_2h").alias("max_island_id_avg_2h"),
    )
    .filter(F.col("version_count") < 1000)
)

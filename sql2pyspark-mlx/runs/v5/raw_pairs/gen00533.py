from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").asc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w3 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

ranked = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

flagged = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w3))

flagged_2 = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = flagged_2.withColumn("session_id", F.sum("session_start_flag").over(w3))

matched_keys = (
    sessionized
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("service") == "auth")
        .select("id")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "service",
        "host",
        "log_ts",
        "value",
        "is_new_burst",
        "session_start_flag",
        F.when(F.col("service").rlike("^(search|auth)$"), "CORE").when(F.col("service").rlike("^(billing|checkout)$"), "GROUP_B").otherwise("PRIORITY").alias("service_zone"),
    )
    .orderBy(F.col("log_ts").desc(), F.col("service").desc())
)

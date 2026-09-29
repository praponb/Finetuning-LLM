from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

flagged = (
    unpacked
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

cleaned = (
    islands
    .filter(F.col("key").isNotNull())
    .withColumn("flag_bit4_times_is_new_burst", (F.col("flag_bit4") * F.col("is_new_burst")))
    .withColumn("resolved_key", F.coalesce("key", F.lit("N/A")))
)

result = (
    cleaned
    .select(
        "id",
        "log_ts",
        "key",
        "value",
        "status_code_high",
        "is_new_burst",
        "flag_bit4_times_is_new_burst",
        "resolved_key",
    )
    .orderBy("id")
)

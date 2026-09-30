from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key", "host").orderBy(F.col("island_id").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("latency_ms") == 1544)
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        "v",
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "log_ts", "key")
)

result = latest.select("id", "log_ts", "key")

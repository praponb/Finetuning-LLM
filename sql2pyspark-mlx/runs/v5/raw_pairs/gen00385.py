from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").asc_nulls_first(), "id", "key")
w_ordered = Window.orderBy(F.col("latency_ms").desc(), "id", "key")
w_recent = Window.partitionBy("level").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w6 = Window.partitionBy("host", "service").orderBy("latency_ms", "id", "key")
w7 = Window.partitionBy("level", "host").orderBy(F.col("latency_ms").desc_nulls_last(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

latest = (
    lines
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w_recent))
    .withColumn("status_code_rank", F.rank().over(w4))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w5))
    .withColumn("prev_latency_ms", F.lag("latency_ms", 1).over(w6))
)

cleaned = (
    with_windows
    .filter(F.col("key").isNotNull() & F.col("latency_ms").between(980, 2357))
    .withColumn("seq_no_filled", F.coalesce("seq_no", F.lit(0)))
)

latest_app_logs = (
    cleaned
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "rn",
        "seq_no",
        "status_code_cume_dist",
        "prev_latency_ms",
    )
)

valid = (
    latest_app_logs
    .filter(F.col("rn") != 1)
    .select("*", F.lower("key").alias("key_lower"))
)

result = (
    valid
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "key",
        "seq_no",
        "status_code_cume_dist",
        F.when(F.col("level").rlike("^(INFO|ERROR)$"), "EXTENDED").when(F.col("level").rlike("^(WARN|DEBUG)$"), "CORE").otherwise("GROUP_A").alias("level_zone"),
    )
)

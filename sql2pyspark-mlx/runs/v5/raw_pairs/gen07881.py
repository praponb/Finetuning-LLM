from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("flag_bit2", "id", "attr_key").rowsBetween(-3, -1)
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc(), "id", "attr_key")
w_ordered = Window.partitionBy("host", "level").orderBy("flag_bit2", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("level", "attr_key").orderBy(F.col("status_code_avg_3_prior").desc())
w6 = Window.partitionBy("attr_key", "level").orderBy(F.col("log_ts").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
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
        "host",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

filtered = (
    unpacked
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        (F.col("flag_bit2") - F.col("status_code_high")).alias("flag_bit2_minus_status_code_high"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "flag_bit2",
        F.avg("status_code").over(w).alias("status_code_avg_3_prior"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("status_code_high").over(w_ordered).alias("status_code_high_avg_6"),
    )
)

base = (
    scored
    .filter(F.col("attr_key").isNotNull() & (F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 05:00:00"))))
    .select("*", F.lower("attr_key").alias("attr_key_lower"))
)

deduped = (
    base
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "status_code_avg_3_prior",
        "status_code_share",
        "event_seq",
        "status_code_high_avg_6",
        "attr_key_lower",
    )
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

wide = (
    ranked
    .groupBy("level")
    .pivot("attr_key", ["region", "trace", "version"])
    .agg(F.sum("status_code_share").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    wide
    .select(
        "level",
        "region_cnt",
        "trace_amount",
        "trace_cnt",
        "version_amount",
        "version_cnt",
    )
    .orderBy(F.col("trace_amount").desc_nulls_last(), F.col("version_cnt").desc())
)

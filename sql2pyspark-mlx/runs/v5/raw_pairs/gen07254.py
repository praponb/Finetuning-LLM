from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "key").orderBy(F.col("status_code").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w_grp = Window.partitionBy("key").orderBy(F.col("dedup_rank").desc(), "id", "key")
w4 = Window.orderBy("status_code_high", "id", "key").rowsBetween(-6, Window.currentRow)
w_part = Window.partitionBy("key").orderBy("dedup_rank", "id", "key").rowsBetween(-6, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    flattened
    .filter(F.col("host") != "h3")
    .select(
        "id",
        "host",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    unpacked
    .filter(F.col("key").isin(["version", "trace"]))
    .withColumn("status_code_high_plus_status_code", (F.col("status_code_high") + F.col("status_code")))
    .withColumn("resolved_key", F.coalesce("key", F.lit("OTHER")))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .filter(F.col("status_code_high") != 1)
    .select(
        "id",
        "host",
        "status_code",
        "key",
        "status_code_high_plus_status_code",
        "dedup_rank",
        F.rank().over(w2).alias("status_code_rank"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.avg("status_code_high").over(w4).alias("status_code_high_avg_6"),
        F.avg("status_code").over(w_part).alias("status_code_avg_6"),
    )
)

result = (
    scored
    .withColumn("status_code_high_avg_6_tier", F.when(F.col("status_code_high_avg_6") >= 2, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["status_code_high_avg_6_tier", "host"], ["status_code_high_avg_6_tier"], []],
        "status_code_high_avg_6_tier",
        "host",
    )
    .agg(
        F.round(F.sum("status_code_high_avg_6") / F.nullif(F.count(F.when(F.col("host") == "h2", 1)), F.lit(0)), 2).alias("status_code_high_avg_6_per_h2"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
)

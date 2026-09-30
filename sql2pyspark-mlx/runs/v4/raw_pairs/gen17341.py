from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("attr_key")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy(F.col("has_fault").asc_nulls_first())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("activity_day").orderBy(F.col("cnt").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("latency_ms") != 919)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

enriched = (
    unpacked
    .select(
        "*",
        F.max("has_fault").over(w_grp).alias("attr_key_max_has_fault"),
        F.avg("has_fault").over(w2).alias("has_fault_avg_1h"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w3).alias("has_fault_percent_rank"),
        F.max("latency_ms").over(w4).alias("latency_ms_max_15m"),
    )
)

stats = (
    scored
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("has_fault_avg_1h") >= 1, "PREMIUM").otherwise("STANDARD").alias("has_fault_avg_1h_tier"),
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("latency_ms_bit_or") >= 1)
    .withColumn("cnt_rank", F.rank().over(w5))
)

result = (
    stats
    .select(
        "activity_day",
        "has_fault_avg_1h_tier",
        "latency_ms_bit_or",
        "cnt",
        "distinct_ids",
    )
)

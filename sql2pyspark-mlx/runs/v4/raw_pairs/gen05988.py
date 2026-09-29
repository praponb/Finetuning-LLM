from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("status_code_high", "id", "k")
w_grp = Window.partitionBy("level", "k").orderBy(F.col("has_fault").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "level",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    unpacked
    .select(
        "*",
        F.row_number().over(w_running).alias("event_seq"),
        F.ntile(4).over(w_grp).alias("has_fault_bucket"),
    )
)

result = (
    metrics
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL"))
    .groupingSets([["event_seq_tier", "k"], ["event_seq_tier"]], "event_seq_tier", "k")
    .agg(
        F.max(F.when(F.col("level") == "INFO", F.col("has_fault"))).alias("info_max_has_fault"),
        F.min("has_fault_bucket").alias("min_has_fault_bucket"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("has_fault")).otherwise(0)).alias("debug_has_fault"),
    )
    .filter(F.col("debug_has_fault") >= 0)
)

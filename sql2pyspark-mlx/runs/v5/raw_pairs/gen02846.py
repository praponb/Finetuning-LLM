from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("has_fault").desc(), "id", "attr_key")
w2 = Window.orderBy("status_code")
w3 = Window.orderBy("has_fault", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_code_high", "id", "attr_key").rowsBetween(-3, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .filter(~F.col("attr_key").isin(["region", "trace"]))
    .select(
        "*",
        F.ntile(4).over(w).alias("has_fault_bucket"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.first("has_fault_bucket", ignorenulls=True).over(w3).alias("first_known_has_fault_bucket"),
        F.avg("has_fault").over(w4).alias("has_fault_avg_3"),
    )
)

result = (
    with_windows_app_logs
    .cube(
        F.when(F.col("status_code_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("status_code_dense_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.avg("first_known_has_fault_bucket").alias("avg_first_known_has_fault_bucket"),
        F.count_if(F.col("level") == "INFO").alias("info_events"),
    )
    .filter(F.col("avg_first_known_has_fault_bucket") > 0)
    .orderBy("avg_first_known_has_fault_bucket")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc(), "id", "key")
w_running = Window.partitionBy("key").orderBy("status_code_high", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .filter(F.col("key") != "region")
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    parsed
    .select(
        "*",
        F.percent_rank().over(w).alias("status_code_high_percent_rank"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(3).over(w_running).alias("status_code_high_bucket"),
    )
)

result = (
    metrics
    .groupBy("key")
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.percentile_approx("has_fault", 0.9).alias("has_fault_p90"),
        F.count_if(F.col("key") == "version").alias("version_events"),
    )
    .orderBy("version_events")
)

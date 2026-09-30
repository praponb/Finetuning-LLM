from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("host")
w4 = Window.orderBy("status_code")
w5 = Window.partitionBy("host").orderBy(F.col("host_max_status_code").desc())
w6 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

with_windows = (
    flattened
    .select(
        "*",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.min("status_code").over(w2).alias("service_min_status_code"),
        F.max("status_code").over(w3).alias("host_max_status_code"),
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.dense_rank().over(w5).alias("host_max_status_code_dense_rank"),
        F.sum("status_code").over(w6).alias("status_code_sum_2h"),
        F.sum("status_code_percent_rank").over(w6).alias("status_code_percent_rank_sum_2h"),
    )
)

grouped = (
    metrics
    .groupBy("service")
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
)

result = (
    grouped
    .select(
        "service",
        F.when(F.col("service").rlike("^(billing|auth)$"), "CORE").when(F.col("service").rlike("^(search|checkout)$"), "EXTENDED").otherwise("GROUP_A").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").desc())
)

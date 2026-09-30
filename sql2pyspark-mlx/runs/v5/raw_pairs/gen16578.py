from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy("status_code")
w4 = Window.partitionBy("service").orderBy("log_ts", "id", "key")
w5 = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service", "key").orderBy(F.col("status_code_high").desc(), "id", "key")
w7 = Window.partitionBy("key").orderBy("status_code_share", "id", "key").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("key", "service").orderBy("status_code_high", "id", "key").rowsBetween(-3, -1)
w_running = Window.partitionBy("key").orderBy("flag_bit4_avg_3_prior", "id", "key").rowsBetween(-6, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)

with_windows = (
    unpacked
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("flag_bit4_count_2h", F.count("flag_bit4").over(w2))
    .withColumn("status_code_rank", F.rank().over(w3))
)

flagged = (
    with_windows
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w4) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("island_id", F.sum("session_start_flag").over(w5))

scored = (
    sessions_app_logs
    .select(
        "id",
        "status_code",
        "key",
        "status_code_high",
        "flag_bit4",
        "status_code_share",
        "flag_bit4_count_2h",
        "status_code_rank",
        F.row_number().over(w6).alias("row_seq"),
        F.avg("island_id").over(w7).alias("island_id_avg_2"),
        F.avg("flag_bit4").over(w8).alias("flag_bit4_avg_3_prior"),
    )
)

with_windows_app_logs = (
    scored
    .select(
        "id",
        "status_code",
        "key",
        "flag_bit4",
        "status_code_share",
        "flag_bit4_count_2h",
        "island_id_avg_2",
        "flag_bit4_avg_3_prior",
        F.max("flag_bit4_avg_3_prior").over(w_running).alias("last7_max_flag_bit4_avg_3_prior"),
        F.count("flag_bit4_avg_3_prior").over(w_running).alias("last7_count_flag_bit4_avg_3_prior"),
        F.avg("flag_bit4_avg_3_prior").over(w_running).alias("last7_avg_flag_bit4_avg_3_prior"),
    )
)

result = (
    with_windows_app_logs
    .rollup("key")
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
        F.count("*").alias("row_count"),
        F.min("island_id_avg_2").alias("min_island_id_avg_2"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("key").desc_nulls_last())
)

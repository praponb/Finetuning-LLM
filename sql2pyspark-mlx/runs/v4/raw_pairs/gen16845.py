from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w4 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy("prev_log_ts", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("prev_log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("attr_key").orderBy(F.col("flag_bit8").desc(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "attr_key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

flagged = (
    flattened_attrs
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

with_prev = islands.withColumn("prev_log_ts", F.lag("log_ts").over(w3))

flagged_2 = (
    with_prev
    .withColumn("is_new_session_2", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands_app_logs = flagged_2.withColumn("burst_id", F.sum("is_new_session_2").over(w4))

gaps = (
    islands_app_logs
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts") - F.lag("prev_log_ts").over(w5) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands_app_logs_2 = gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w6))

latest = (
    islands_app_logs_2
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "prev_log_ts").alias("year_start"))
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.sum(F.when(F.col("service") == "search", F.col("status_code_high")).otherwise(0)).alias("search_status_code_high"),
        F.percentile_approx("status_code_high", 0.9).alias("status_code_high_p90"),
        F.percentile_approx("is_new_session", 0.9).alias("is_new_session_p90"),
    )
    .filter(F.col("is_new_session_p90") >= 0)
)

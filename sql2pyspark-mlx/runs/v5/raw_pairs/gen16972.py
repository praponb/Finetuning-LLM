from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service")
w_running = Window.partitionBy("level", "service").orderBy(F.col("status_code").desc(), "id", "key")
w3 = Window.partitionBy("service").orderBy(F.col("greatest_status_code_high_is_flagged").desc_nulls_first())
w4 = Window.partitionBy("service", "level")
w5 = Window.partitionBy("key").orderBy("greatest_status_code_high_is_flagged_dense_rank", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("row_seq", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("service").orderBy("last_known_service_max_greatest_status_code_high_is_flagged", "id", "key").rowsBetween(-6, -1)
w8 = Window.partitionBy("level").orderBy("status_code", "id", "key")
w9 = Window.orderBy("greatest_status_code_high_is_flagged", "id", "key").rowsBetween(-3, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    unpacked
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "key",
        "value",
        "is_flagged",
        "status_code_high",
        F.greatest("status_code_high", "is_flagged").alias("greatest_status_code_high_is_flagged"),
        F.when(F.col("level").isin(["DEBUG", "WARN"]), "Priority").otherwise("Other").alias("level_group"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("is_flagged") > 0)
    .withColumn("greatest_status_code_high_is_flagged_tier", F.when(F.col("greatest_status_code_high_is_flagged") >= 74, "High").when(F.col("greatest_status_code_high_is_flagged") >= 39, "Medium").otherwise("Low"))
)

with_windows = (
    filtered_app_logs
    .select(
        "*",
        F.avg("status_code_high").over(w_ordered).alias("service_avg_status_code_high"),
        F.row_number().over(w_running).alias("row_seq"),
        F.dense_rank().over(w3).alias("greatest_status_code_high_is_flagged_dense_rank"),
        F.max("greatest_status_code_high_is_flagged").over(w4).alias("service_max_greatest_status_code_high_is_flagged"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.last("service_max_greatest_status_code_high_is_flagged", ignorenulls=True).over(w5).alias("last_known_service_max_greatest_status_code_high_is_flagged"),
        F.sum("status_code_high").over(w6).alias("cumulative_status_code_high"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "value",
        "is_flagged",
        "status_code_high",
        "level_group",
        "greatest_status_code_high_is_flagged_tier",
        "service_avg_status_code_high",
        "greatest_status_code_high_is_flagged_dense_rank",
        "service_max_greatest_status_code_high_is_flagged",
        "last_known_service_max_greatest_status_code_high_is_flagged",
        "cumulative_status_code_high",
        F.avg("greatest_status_code_high_is_flagged").over(w7).alias("greatest_status_code_high_is_flagged_avg_6_prior"),
        F.ntile(4).over(w8).alias("status_code_bucket"),
        F.avg("last_known_service_max_greatest_status_code_high_is_flagged").over(w9).alias("last_known_service_max_greatest_status_code_high_is_flagged_avg_3"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level", "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_code", "id", "attr_key")
w_grp = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w_part = Window.partitionBy("attr_key").orderBy("level_stddev_samp_status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("row_seq", "id", "attr_key")
w8 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

filtered = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        F.when(F.col("service").isin(["billing", "checkout"]), "Core").otherwise("Other").alias("service_group"),
    )
)

cleaned = (
    filtered
    .filter(F.col("status_code").between(261, 296))
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "service_group",
        F.concat("attr_key", F.lit("/"), "level").alias("attr_key_level"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("resolved_attr_key"),
    )
)

scored = (
    cleaned
    .filter(F.col("log_ts") < "2023-10-01")
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "attr_key_level",
        "resolved_attr_key",
        F.last("attr_key", ignorenulls=True).over(w).alias("last_known_attr_key"),
        F.stddev_samp("status_code").over(w2).alias("level_stddev_samp_status_code"),
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

with_windows = (
    scored
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w_part))
    .withColumn("row_seq_bucket", F.ntile(4).over(w7))
)

with_prev = with_windows.withColumn("prev_log_ts", F.lag("log_ts").over(w8))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w))

result = (
    islands
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "attr_key_level",
        "resolved_attr_key",
        "last_known_attr_key",
        "level_stddev_samp_status_code",
        "first_known_attr_key",
        F.when(F.col("level").rlike("^(WARN|ERROR)$"), "OTHER_ZONE").when(F.col("level").rlike("^(INFO|DEBUG)$"), "EXTENDED").otherwise("CORE").alias("level_zone"),
    )
)

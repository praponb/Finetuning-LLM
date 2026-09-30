from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy("session_no", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("session_no").desc(), "id", "attr_key")
w5 = Window.partitionBy("host").orderBy(F.col("session_no_bucket").desc_nulls_first(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("attr_key").isNotNull() & (F.col("status_code") != 312))
    .select("*", F.concat_ws(",", "host", "attr_key").alias("host_attr_key_key"))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

enriched = (
    sessionized
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        "host_attr_key_key",
        "session_start_flag",
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key"),
        F.ntile(3).over(w4).alias("session_no_bucket"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "status_code",
        "attr_key",
        "attr_value",
        "host_attr_key_key",
        "session_start_flag",
        "first_known_attr_key",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "PRIORITY").when(F.col("attr_key").rlike("^(trace)$"), "CORE").otherwise("GROUP_A").alias("attr_key_zone"),
    )
)

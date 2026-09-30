from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "key").orderBy(F.col("status_code").desc(), "id", "key")
w2 = Window.orderBy("status_code", "id", "key").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("status_code").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy("status_code", "id", "key")
w5 = Window.partitionBy("key", "service").orderBy("status_code", "id", "key")
w6 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w7 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("service", "key")
w9 = Window.partitionBy("service").orderBy("next_status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

ranked = (
    flattened
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "service", "log_ts", "status_code", "key", "v")
)

with_windows = (
    ranked
    .filter(F.col("status_code") <= 272)
    .select(
        "*",
        F.avg("status_code").over(w2).alias("status_code_avg_2_prior"),
        F.ntile(3).over(w3).alias("status_code_bucket"),
        F.lag("status_code", 2, 0).over(w4).alias("prev_status_code"),
        F.lead("status_code", 1).over(w5).alias("next_status_code"),
    )
)

filtered = (
    with_windows
    .filter(F.col("key").isNotNull() & F.col("next_status_code").between(262, 389))
    .select("*", F.initcap(F.trim("service")).alias("service_title"))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w6).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w6) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w7))

with_windows_app_logs = (
    islands
    .select(
        "*",
        F.round((F.col("prev_status_code") / F.sum("prev_status_code").over(w8)), 4).alias("prev_status_code_share"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "key",
        "v",
        "status_code_bucket",
        "prev_status_code",
        "next_status_code",
        F.when(F.col("service").rlike("^(search|checkout)$"), "EXTENDED").when(F.col("service").rlike("^(auth|billing)$"), "GROUP_A").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy(F.col("log_ts").desc_nulls_last(), "key")
)

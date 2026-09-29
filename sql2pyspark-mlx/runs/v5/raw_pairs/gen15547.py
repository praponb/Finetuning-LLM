from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("session_start_flag").desc(), "id", "key")
w4 = Window.partitionBy("host", "key").orderBy("session_start_flag", "id", "key")
w_ordered = Window.partitionBy("host").orderBy("event_seq", "id", "key")
w6 = Window.partitionBy("key").orderBy(F.col("seq_no").desc(), "id", "key")
w7 = Window.orderBy("session_start_flag", "id", "key")
w8 = Window.partitionBy("key").orderBy("seq_no", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

gaps = (
    exploded
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

scored = (
    sessionized
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

metrics = (
    scored
    .select(
        "id",
        "log_ts",
        "key",
        "session_start_flag",
        "seq_no",
        F.lead("island_id").over(w_ordered).alias("next_island_id"),
        F.lag("session_start_flag").over(w_ordered).alias("prev_session_start_flag"),
        F.lead("session_start_flag").over(w_ordered).alias("next_session_start_flag"),
        F.row_number().over(w_ordered).alias("seq_no_2"),
    )
)

scored_app_logs = (
    metrics
    .select(
        "id",
        "key",
        "session_start_flag",
        "seq_no_2",
        F.ntile(4).over(w6).alias("seq_no_bucket"),
        F.ntile(5).over(w7).alias("session_start_flag_bucket"),
        F.first("prev_session_start_flag", ignorenulls=True).over(w8).alias("first_known_prev_session_start_flag"),
    )
)

result = (
    scored_app_logs
    .select(
        "id",
        "key",
        "session_start_flag",
        "seq_no_2",
        "seq_no_bucket",
        "session_start_flag_bucket",
        "first_known_prev_session_start_flag",
        F.when(F.col("key").rlike("^(region|version)$"), "CORE").otherwise("EXTENDED").alias("key_zone"),
    )
)

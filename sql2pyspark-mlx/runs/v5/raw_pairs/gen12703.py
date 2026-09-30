from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_context").desc_nulls_first())
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.orderBy(F.col("num_context").desc(), "id")
w5 = Window.orderBy("num_context", "id").rowsBetween(-2, -1)

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["version"].alias("version"),
        F.size("context").alias("num_context"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "log_ts",
        "version",
        "num_context",
        F.cume_dist().over(w).alias("num_context_cume_dist"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("num_context").over(w5).alias("num_context_avg_2_prior"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.avg("duration_sec").over(w_running).alias("duration_sec_avg_6"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)

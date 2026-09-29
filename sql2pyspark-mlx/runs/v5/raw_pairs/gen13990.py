from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("service", "level").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.partitionBy("level", "service").orderBy("duration_sec", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "service",
        "level",
        "user_tier",
        "duration_sec",
        F.avg("duration_sec").over(w_hist).alias("duration_sec_avg_5"),
        F.lag("duration_sec", 1, 0).over(w_ordered).alias("prev_duration_sec"),
        F.lead("duration_sec", 1).over(w_ordered).alias("next_duration_sec"),
    )
)

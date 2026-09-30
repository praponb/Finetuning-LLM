from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier").orderBy("duration_sec", "id")
w2 = Window.partitionBy("host", "tier").orderBy("flag_bit2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "context",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "host",
        "flag_bit2",
        F.lead("flag_bit2").over(w).alias("next_flag_bit2"),
        F.lag("flag_bit2").over(w).alias("prev_flag_bit2"),
        F.lead("duration_sec").over(w).alias("next_duration_sec"),
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)

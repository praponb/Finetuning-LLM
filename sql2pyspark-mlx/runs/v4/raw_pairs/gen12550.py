from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("retry_count").desc(), "id")
w2 = Window.partitionBy("level").orderBy("retry_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("flag_bit8").desc())
w4 = Window.partitionBy("level").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("running_flag_bit8", F.sum("flag_bit8").over(w2))
    .withColumn("flag_bit8_percent_rank", F.percent_rank().over(w3))
    .withColumn("first_known_flag_bit8", F.first("flag_bit8", ignorenulls=True).over(w4))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level", "tier").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.hour("log_ts").between(11, 15))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("flag_bit2").over(w_ordered).alias("running_flag_bit2"),
        F.last("tier", ignorenulls=True).over(w_ordered).alias("last_known_tier"),
        F.sum("flag_bit2").over(w2).alias("cumulative_flag_bit2"),
        F.sum("flag_bit2").over(w3).alias("running_flag_bit2_2"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("flag_bit8").desc_nulls_last())

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.sum("status_code_high").over(w).alias("cumulative_status_code_high"),
        F.first("flag_bit8", ignorenulls=True).over(w2).alias("first_known_flag_bit8"),
        F.percent_rank().over(w3).alias("flag_bit8_percent_rank"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_context").desc(), "id")
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w3 = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

result = (
    parsed
    .withColumn("num_context_bucket", F.ntile(5).over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("first_known_latency_ms", F.first("latency_ms", ignorenulls=True).over(w3))
)

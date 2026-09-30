from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("host", "service").orderBy("log_ts", "id")
w_ordered = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context").getItem("trace").alias("trace"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    parsed
    .filter(F.col("service") == "search")
    .select(
        "*",
        F.lead("latency_ms").over(w_hist).alias("next_latency_ms"),
        F.lag("latency_ms").over(w_hist).alias("prev_latency_ms"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.first("trace", ignorenulls=True).over(w_ordered).alias("first_known_trace"),
    )
)

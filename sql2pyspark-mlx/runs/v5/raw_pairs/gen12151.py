from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("host").orderBy("num_context", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("service").isin(["billing", "search"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "payload_json",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.lead("num_context").over(w_recent).alias("next_num_context"),
        F.lag("num_context").over(w_recent).alias("prev_num_context"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

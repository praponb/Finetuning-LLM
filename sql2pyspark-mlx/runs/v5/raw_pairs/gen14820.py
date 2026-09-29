from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("has_fault", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.lag("has_fault").over(w).alias("prev_has_fault"),
        F.row_number().over(w).alias("seq_no"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w2 = Window.orderBy("status_code", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w_ordered = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "latency_ms",
        "attr_key",
        F.ntile(4).over(w).alias("status_code_bucket"),
        F.ntile(3).over(w2).alias("status_code_bucket_2"),
        F.row_number().over(w3).alias("row_seq"),
        F.corr("latency_ms", "status_code").over(w_ordered).alias("corr_latency_ms_status_code"),
    )
)

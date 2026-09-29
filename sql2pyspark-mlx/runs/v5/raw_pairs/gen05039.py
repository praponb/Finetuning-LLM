from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("num_context").desc())

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "log_ts",
        "num_context",
        "version_or_default",
        F.avg("status_code").over(w).alias("status_code_avg_5_prior"),
        F.rank().over(w2).alias("num_context_rank"),
    )
)

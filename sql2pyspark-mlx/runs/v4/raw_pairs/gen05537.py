from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
    )
)

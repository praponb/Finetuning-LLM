from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .groupBy(
        "level",
        F.when(F.col("status_code") >= 439, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.avg("is_flagged").alias("avg_is_flagged"),
    )
    .filter(F.col("avg_is_flagged") >= 0)
    .orderBy("status_code_tier")
)

from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.col("context").getItem("trace").alias("trace"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("num_context") >= 60, "HIGH").otherwise("LOW").alias("num_context_tier"),
        F.when(F.col("latency_ms") >= 2505, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.sum("num_context").alias("total_num_context"),
        F.round(F.avg("num_context"), 2).alias("avg_num_context"),
    )
    .orderBy(F.col("status_code_p50").desc_nulls_last())
)

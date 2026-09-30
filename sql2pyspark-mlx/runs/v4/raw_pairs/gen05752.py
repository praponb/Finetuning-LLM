from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        "attr_key",
        F.when(F.col("status_code") >= 452, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("status_code_per_search"),
        F.min("status_code").alias("min_status_code"),
    )
)

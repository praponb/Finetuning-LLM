from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy("level", "service")
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles_2"),
    )
)

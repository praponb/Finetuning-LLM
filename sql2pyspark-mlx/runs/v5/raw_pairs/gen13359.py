from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "level",
        "status_code",
        F.when(F.col("level").rlike("^(DEBUG|WARN)$"), "OTHER_ZONE").when(F.col("level").rlike("^(INFO|ERROR)$"), "CORE").otherwise("GROUP_B").alias("level_zone"),
    )
)

from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "host", "level", F.explode("context").alias("key", "v"))
)

filtered = (
    lines
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "level",
        "key",
        "v",
        F.initcap(F.trim("service")).alias("service_title"),
    )
)

base = (
    filtered
    .filter(F.col("level") == "WARN")
    .select("*", F.upper("level").alias("level_upper"))
)

valid = (
    base
    .filter(~F.col("level").isin(["ERROR", "WARN", "DEBUG"]))
    .select(
        "id",
        "service",
        "level",
        "key",
        "service_title",
        "level_upper",
        F.initcap(F.trim("level")).alias("level_title"),
        F.when(F.col("level").isin(["ERROR", "INFO"]), "Core").otherwise("Other").alias("level_group"),
    )
)

result = (
    valid
    .select("id", "level", "key", "service_title", "level_title", "level_group")
    .orderBy(F.col("level").desc(), "key")
)

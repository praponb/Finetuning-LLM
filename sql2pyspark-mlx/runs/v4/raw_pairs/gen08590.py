from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat_ws("/", "browser", "device").alias("browser_device_key"),
        F.lower("device").alias("device_lower"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("resolved_country"),
    )
)

result = (
    base
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(F.col("country") == "JP")
        .select("country", "browser")
    )
)

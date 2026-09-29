from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "desktop", "tablet"]))
    .select(
        "id",
        "device",
        "started_at",
        "browser",
        F.substring("browser", 1, 3).alias("browser_prefix"),
    )
)

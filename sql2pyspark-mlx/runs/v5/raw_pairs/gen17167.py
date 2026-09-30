from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "browser",
        "country",
        "device",
        F.concat_ws(",", "browser", "device").alias("browser_device_key"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
    .orderBy("started_at_day")
)

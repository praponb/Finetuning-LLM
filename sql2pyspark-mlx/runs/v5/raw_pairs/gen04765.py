from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "desktop"]))
    .select(
        "id",
        "started_at",
        "country",
        "device",
        F.lower("browser").alias("browser_lower"),
        F.length("country").alias("country_length"),
    )
)

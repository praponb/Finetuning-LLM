from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "tablet") & (F.col("device") == "mobile") & F.col("country").isNotNull())
    .select(
        "id",
        "browser",
        F.trim("browser").alias("clean_browser"),
        F.upper("device").alias("device_upper"),
    )
)

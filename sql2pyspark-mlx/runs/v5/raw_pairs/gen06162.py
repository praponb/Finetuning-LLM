from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") != "TH")
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

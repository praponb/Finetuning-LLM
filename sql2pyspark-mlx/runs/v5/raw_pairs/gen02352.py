from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

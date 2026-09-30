from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
    )
)

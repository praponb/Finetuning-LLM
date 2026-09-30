from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        F.coalesce("country", F.lit("unassigned")).alias("country_filled"),
    )
)

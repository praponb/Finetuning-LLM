from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Firefox") & ((F.col("started_at") >= "2025-09-01") | (F.col("device") == "mobile")))
    .select(
        "country",
        F.coalesce("browser", F.lit("unassigned")).alias("browser_filled"),
    )
)

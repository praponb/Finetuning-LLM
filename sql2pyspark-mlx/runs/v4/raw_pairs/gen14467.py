from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-14").cast("date")) & F.col("browser").isNotNull() & (F.col("started_at") < "2024-02-24"))
    .select(
        "id",
        "device",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

result = (
    cleaned
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2023-05-01")
        .select("device")
    )
)

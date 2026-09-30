from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & F.col("country").isin(["TH", "JP"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.date_trunc("week", "started_at").alias("started_at_period"),
        F.coalesce("country", F.lit("OTHER")).alias("country_filled"),
    )
)

result = (
    cleaned
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(F.to_date("started_at") < "2025-03-01")
        .select("country", "browser")
    )
)

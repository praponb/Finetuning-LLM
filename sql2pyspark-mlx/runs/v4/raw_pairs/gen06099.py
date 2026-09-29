from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-27").cast("date")) & F.col("browser").isNotNull() & ~F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.upper("device").alias("device_upper"),
    )
)

result = (
    filtered
    .select("device", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("device", "country")
    )
)

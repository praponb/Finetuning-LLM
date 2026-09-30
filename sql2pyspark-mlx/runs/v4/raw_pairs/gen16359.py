from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.upper("browser").alias("browser_upper"),
    )
)

result = (
    cleaned
    .select("id", "device")
    .intersect(
        df_sessions
        .filter(F.col("country").isNotNull())
        .select("id", "device")
    )
)

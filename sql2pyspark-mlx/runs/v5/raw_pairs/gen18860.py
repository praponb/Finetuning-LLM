from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.lower("device").alias("device_lower"),
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

result = (
    base
    .select("device", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isin(["US", "SG"]))
        .select("device", "id")
    )
)

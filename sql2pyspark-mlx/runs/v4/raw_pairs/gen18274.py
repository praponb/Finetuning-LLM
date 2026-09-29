from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "country",
        "started_at",
        F.trim("country").alias("clean_country"),
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
    )
)

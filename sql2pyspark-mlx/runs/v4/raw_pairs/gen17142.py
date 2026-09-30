from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Firefox", "Chrome"]))
    .select(
        "id",
        "country",
        "started_at",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)

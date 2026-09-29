from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "started_at",
        "browser",
        "customer_id",
        F.hour("started_at").alias("started_at_hour"),
    )
    .orderBy("browser", F.col("started_at_hour").desc())
)

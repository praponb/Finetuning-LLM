from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.lower("browser").alias("browser_lower"),
    )
    .orderBy("country")
)

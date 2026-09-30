from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-10-15 21:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .withColumn("clean_country", F.trim("country"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("OTHER")))
)

result = (
    valid
    .select("device", "country")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["US", "DE", "TH"]))
        .select("device", "country")
    )
)

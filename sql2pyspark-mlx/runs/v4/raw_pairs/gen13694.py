from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["UK", "SG", "US"]) & F.col("country").isin(["US", "UK", "DE"]) & (F.col("started_at") >= "2023-07-01"))
    .select(
        "id",
        "country",
        "device",
        F.concat("country", F.lit(" - "), "browser").alias("country_browser"),
    )
    .orderBy("id")
    .limit(10)
)

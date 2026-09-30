from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("started_at") >= "2024-06-01")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("country", ["UK", "DE"])
    .agg(F.sum("browser_length").alias("total"), F.count(F.lit(1)).alias("cnt"))
)

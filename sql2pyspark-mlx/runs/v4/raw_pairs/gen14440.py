from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "customer_id",
        F.concat("browser", F.lit(" - "), "device").alias("browser_device"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
    .orderBy("country_filled", "browser_device")
)

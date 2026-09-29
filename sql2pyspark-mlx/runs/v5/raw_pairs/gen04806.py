from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-07-16 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat("device", F.lit(" - "), "browser").alias("device_browser"),
        F.length("device").alias("device_length"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["customer_country", "browser"], ["customer_country"], []],
        F.col("dim.country").alias("customer_country"),
        F.col("src.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.device_length")).alias("max_device_length"),
    )
)

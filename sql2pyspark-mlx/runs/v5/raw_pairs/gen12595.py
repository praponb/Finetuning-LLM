from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("browser").isin(["Firefox", "Edge", "Safari"]))
    .withColumn("browser_lower", F.lower("browser"))
    .withColumn("country_filled", F.coalesce("country", F.lit("OTHER")))
)

result = (
    filtered
    .select("customer_id", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isin(["SG", "TH", "US"]))
        .select("customer_id", "country")
    )
)

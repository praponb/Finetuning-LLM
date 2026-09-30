from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-02-17 14:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.initcap(F.trim("country")).alias("country_title"),
        F.substring("country", 1, 4).alias("country_prefix"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.age"), F.col("b.country").alias("customer_country"))
)

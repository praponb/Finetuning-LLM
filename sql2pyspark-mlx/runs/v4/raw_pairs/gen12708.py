from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-07-16").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("country").isin(["UK", "TH"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_filled"),
    )
)

result = (
    cleaned
    .groupBy("country")
    .pivot("city", ["New York", "Berlin", "Tokyo"])
    .agg(F.max("age"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Berlin").alias("berlin"),
        F.col("Tokyo").alias("tokyo"),
    )
)

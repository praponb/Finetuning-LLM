from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2024-02-04").cast("date"))
    .withColumn("discount_pct_whole", F.floor("discount_pct"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

filtered_promotions = (
    filtered
    .filter((F.col("ends_on") > F.lit("2023-08-27").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "discount_pct_whole",
        "resolved_channel",
        F.least("discount_pct_whole", "discount_pct").alias("least_discount_pct_whole_discount_pct"),
    )
)

cleaned = (
    filtered_promotions
    .filter(F.col("discount_pct") > 0.21)
    .select(
        "id",
        "discount_pct",
        "discount_pct_whole",
        "resolved_channel",
        "least_discount_pct_whole_discount_pct",
        F.when(F.col("discount_pct_whole") >= 82, "High").when(F.col("discount_pct_whole") >= 33, "Medium").otherwise("Low").alias("discount_pct_whole_tier"),
    )
)

result = cleaned.select("id", "discount_pct", "resolved_channel", "discount_pct_whole_tier")

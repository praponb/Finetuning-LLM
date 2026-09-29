from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-12-19").cast("date"))
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("starts_on", "MMM yyyy").alias("starts_on_label"),
    )
)

base = (
    valid
    .filter(F.col("ends_on").between("2023-09-01", "2025-02-02"))
    .withColumn("discount_pct_rounded", F.round("discount_pct", 1))
)

cleaned = (
    base
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_label",
        F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"),
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label_2"),
    )
)

summary = (
    cleaned
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count("*").alias("event_count"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
)

result = (
    summary
    .select("activity_month", "quarter_start", "avg_discount_pct")
    .orderBy(F.col("quarter_start").desc_nulls_first())
)

from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

valid = (
    flattened
    .filter(F.col("val") > 47)
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

kept_keys = (
    valid
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-06-01", "2024-03-01"))
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("effective_from") > F.lit("2024-06-16").cast("date"))
    .withColumn("plan_prefix", F.substring("plan", 1, 2))
)

stats = (
    cleaned
    .groupBy(
        F.when(F.col("val") >= 87, "HIGH").otherwise("LOW").alias("val_tier"),
        "plan",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.sum(F.when(F.col("plan") == "pro", F.col("val"))).alias("pro_total_val"),
    )
)

result = stats.select("val_tier", "plan", "distinct_ids", "val_p50")
